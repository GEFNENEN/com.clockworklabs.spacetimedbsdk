#if WEIXINMINIGAME
using SpacetimeDB.BSATN;
using SpacetimeDB.ClientApi;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WebSocketState = UnityWebSocket.WebSocketState;


namespace SpacetimeDB
{
    internal class WebSocket
    {
        public delegate void OpenEventHandler();

        public delegate void MessageEventHandler(byte[] message, DateTime timestamp);

        public delegate void CloseEventHandler(Exception? e);

        public delegate void ConnectErrorEventHandler(Exception e);

        public delegate void SendErrorEventHandler(Exception e);

        public struct ConnectOptions
        {
            public string Protocol;
        }

        // WebSocket buffer for incoming messages
        private static readonly int MAXMessageSize = 0x4000000; // 64MB

        // Connection parameters
        private readonly ConnectOptions _options;
        private readonly byte[] _receiveBuffer = new byte[MAXMessageSize];
        private readonly ConcurrentQueue<Action> dispatchQueue = new();

        protected UnityWebSocket.WebSocket? Ws;

        public WebSocket(ConnectOptions options)
        {
            this._options = options;
        }

        public event OpenEventHandler? OnConnect;
        public event ConnectErrorEventHandler? OnConnectError;
        public event SendErrorEventHandler? OnSendError;

        /// <summary>
        ///  Called directly by background task (not on main thread!)
        /// </summary>
        public event MessageEventHandler? OnMessage;

        public event CloseEventHandler? OnClose;


        public bool IsConnected => this.Ws is { ReadyState: WebSocketState.Open };

        public async Task Connect(string? auth, string host, string nameOrAddress, ConnectionId connectionId, Compression compression, bool light)
        {
            var uri = $"{host}/v1/database/{nameOrAddress}/subscribe?connection_id={connectionId}&compression={compression}";
            UnityEngine.Debug.Log($"url: {uri}");
            if (light)
            {
                uri += "&light=true";
            }

            this.Ws = new(uri, this._options.Protocol);

            this.Ws.OnOpen += (_, _) =>
            {
                UnityEngine.Debug.Log("WebSocket connection opened.");
                if (this.OnConnect != null)
                {
                    this.dispatchQueue.Enqueue(() => this.OnConnect());
                }
            };


            this.Ws.OnMessage += (_, e) =>
            {
                UnityEngine.Debug.Log($"WebSocket message received. IsBinary: {e.IsBinary}, DataLength: {e.Data.Length}, RawDataLength: {e.RawData.Length}");
                if (!e.IsBinary)
                {
                    UnityEngine.Debug.LogError($"WebSocket received non-binary message: {e.Data}");
                    return;
                }

                if (this.OnMessage == null)
                {
                    UnityEngine.Debug.LogWarning($"WebSocket received message but no handler is registered. Message: {e.Data}");
                    return;
                }

                try
                {
                    this.OnMessage(e.RawData, DateTime.UtcNow);
                }
                catch (WebSocketException ex)
                {
                    if (this.OnClose != null) this.dispatchQueue.Enqueue(() => this.OnClose(ex));
                }
            };

            this.OnClose += (e) =>
            {
                if (e == null)
                {
                    UnityEngine.Debug.Log("WebSocket connection closed normally.");
                }
                else
                {
                    UnityEngine.Debug.LogError($"WebSocket connection closed with error: {e.Message}");
                }

                if (this.OnClose != null) this.dispatchQueue.Enqueue(() => this.OnClose(e));
            };

            this.Ws.OnError += (_, e) =>
            {
                UnityEngine.Debug.LogError($"WebSocket error: {e.Message}");
                if (this.OnConnectError != null) this.dispatchQueue.Enqueue(() => this.OnConnectError(new Exception(e.Message)));
            };


            if (!string.IsNullOrEmpty(auth))
            {
                // this.Ws.Options.SetRequestHeader("Authorization", $"Bearer {auth}");
            }
            else
            {
                // this.Ws.Options.UseDefaultCredentials = true;
            }

            this.Ws.ConnectAsync();

            await Task.CompletedTask;
        }

        public Task Close(WebSocketCloseStatus code = WebSocketCloseStatus.NormalClosure)
        {
            // if (this.Ws?.State == WebSocketState.Open)
            // {
            //     return this.Ws.CloseAsync(code, "Disconnecting normally.", CancellationToken.None);
            // }

            this.Ws?.CloseAsync();

            return Task.CompletedTask;
        }

        private Task? senderTask;
        private readonly ConcurrentQueue<ClientMessage> messageSendQueue = new();

        /// <summary>
        /// This sender guarantees that that messages are sent out in the order they are received. Our websocket
        /// library only allows us to await one send call, so we have to wait until the current send call is complete
        /// before we start another one. This function is also thread safe, just in case.
        /// </summary>
        /// <param name="message">The message to send</param>
        public void Send(ClientMessage message)
        {
            lock (this.messageSendQueue)
            {
                this.messageSendQueue.Enqueue(message);
#if UNITY_WEBGL && !UNITY_EDITOR
                if (SpacetimeDBNetworkManager._instance != null)
                    SpacetimeDBNetworkManager._instance.StartCoroutine(this.ProcessSendQueue());
#else
                this.senderTask ??= Task.Run(this.ProcessSendQueue);
#endif
            }
        }

#if UNITY_WEBGL && !UNITY_EDITOR
        private IEnumerator ProcessSendQueue()
#else
        private async Task ProcessSendQueue()
#endif
        {
#if UNITY_WEBGL && !UNITY_EDITOR
#else
            await Task.Yield(); // Ensure we are not blocking the main thread
#endif
            while (true)
            {
                ClientMessage message;

                lock (this.messageSendQueue)
                {
                    if (!this.messageSendQueue.TryDequeue(out message))
                    {
                        // We are out of messages to send
                        this.senderTask = null;

#if UNITY_WEBGL && !UNITY_EDITOR
                        yield break;
#else
                        return;
#endif
                    }
                }

                try
                {
                    var messageBSATN = new ClientMessage.BSATN();
                    var encodedMessage = IStructuralReadWrite.ToBytes(messageBSATN, message);
                    this.Ws!.SendAsync(encodedMessage);
                }
                catch (Exception e)
                {
                    this.senderTask = null;
                    if (this.OnSendError != null) this.dispatchQueue.Enqueue(() => this.OnSendError(e));
                }
            }
        }

        public WebSocketState GetState()
        {
            return this.Ws!.ReadyState;
        }

        public void Update()
        {
            while (this.dispatchQueue.TryDequeue(out var result))
            {
                result();
            }
        }
    }
}
#endif