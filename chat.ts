import {
  createChatThread,
  fetchAnonymousToken,
  generateDeviceID,
  getSignedWsUrl,
  refreshAuthToken,
  uploadFile,
} from './core/types'
import type { ChatRequestMessage, ChatResponseStream } from './core/chat-types'

const DEFAULT_AGENT_ID = '6812e64f9dfaf301f7000001'

export class User {
  #accessToken: string
  readonly deviceId: string
  #refreshToken: string
  #refreshPromise: Promise<void> | null = null
  #refreshTimer: ReturnType<typeof setTimeout> | null = null
  get accessToken(): string {
    return this.#accessToken
  }
  constructor(deviceId: string, accessToken: string, refreshToken: string, expiresAt: number | null = null) {
    this.#accessToken = accessToken
    this.deviceId = deviceId
    this.#refreshToken = refreshToken
    if (expiresAt !== null) {
      this.#scheduleRefresh(expiresAt)
    }
  }
  static async create(): Promise<User> {
    const deviceId = generateDeviceID()
    const i = await fetchAnonymousToken(deviceId)
    return new User(deviceId, i.accessToken, i.refreshToken, i.expiresAt)
  }
  #scheduleRefresh(expiresAt: number): void {
    if (this.#refreshTimer !== null) {
      clearTimeout(this.#refreshTimer)
    }
    // 有効期限の5分前にリフレッシュ (すでに5分以内なら即時)
    const delay = Math.max(0, expiresAt - Date.now() - 5 * 60 * 1000)
    this.#refreshTimer = setTimeout(() => {
      this.#refreshTimer = null
      this.refresh().catch(() => {})
    }, delay)
  }
  async refresh(): Promise<void> {
    if (this.#refreshPromise) return this.#refreshPromise
    this.#refreshPromise = this.#doRefresh()
    try {
      await this.#refreshPromise
    } finally {
      this.#refreshPromise = null
    }
  }
  async #doRefresh(): Promise<void> {
    const tokens = await refreshAuthToken(this.deviceId, this.#refreshToken)
    this.#accessToken = tokens.accessToken
    this.#refreshToken = tokens.refreshToken
    if (tokens.expiresAt !== null) {
      this.#scheduleRefresh(tokens.expiresAt)
    }
  }
  async #withTokenRefresh<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn()
    } catch (err) {
      if (err instanceof Error && (err.message.includes('Invalid token') || err.message.includes('HTTP Error 401'))) {
        await this.refresh()
        return await fn()
      }
      throw err
    }
  }
  async createThread(opts?: { scenarioAgentId?: string; title?: string }): Promise<Thread> {
    const thread = await this.#withTokenRefresh(() =>
      createChatThread(this.deviceId, this.#accessToken, {
        scenarioAgentId: opts?.scenarioAgentId ?? DEFAULT_AGENT_ID,
        title: opts?.title ?? '新しいスレッド',
      })
    )
    const connected = await Thread.connect(thread.id, this)
    return connected
  }
  async uploadFile(opts: {
    file: File
    threadId?: string
    isImage?: boolean
  }): Promise<UploadedFile> {
    const res = await this.#withTokenRefresh(() =>
      uploadFile(this.deviceId, this.#accessToken, {
        file: opts.file,
        threadId: opts.threadId ?? crypto.randomUUID(),
        isImage: opts.isImage,
      })
    )
    return {
      fileId: res.fileId,
      fileUrl: res.fileUrl,
      fileName: res.originalFilename,
      isImage: Boolean(opts.isImage),
    }
  }
}
export interface UploadedFile {
  fileId: string
  fileUrl: string
  fileName: string
  isImage: boolean
}
/* TODO: support multiple conversations */
export class Thread {
  readonly id: string
  #user: User
  #ws: WebSocket
  #stream: ReadableStream<ChatResponseStream>
  #streamReader: ReadableStreamDefaultReader<ChatResponseStream>
  #disposed = false
  #pendingReject: ((err: unknown) => void) | null = null
  constructor(id: string, user: User, ws: WebSocket) {
    this.id = id
    this.#user = user
    this.#ws = ws

    this.#stream = new ReadableStream<ChatResponseStream>({
      start: async (controller) => {
        while (!this.#disposed) {
          try {
            // 現在のWSの終了を待機するPromiseを作成
            const closed = new Promise<void>((resolve, reject) => {
              this.#pendingReject = reject
              // FIXME: 場合によっては初っ端のメッセージを取りこぼすけど知らない
              this.#ws.onmessage = (e) => {
                try {
                  controller.enqueue(JSON.parse(e.data) as ChatResponseStream)
                } catch (err) {
                  reject(new Error('Failed to parse message'))
                }
              }
              this.#ws.onerror = reject
              this.#ws.onclose = async () => {
                const oldWs = this.#ws
                try {
                  this.#ws = await Thread.connectWS(this.#user)
                } catch {
                  // WS接続失敗時にトークンリフレッシュを試行して再接続
                  // NOTE: WS APIではHTTPステータスを取得できないため、
                  // トークン期限切れ以外のエラーでもリフレッシュが走る
                  try {
                    await this.#user.refresh()
                    this.#ws = await Thread.connectWS(this.#user)
                  } catch (err) {
                    reject(err)
                    return
                  }
                }
                // 旧WSのハンドラをクリーンアップ
                oldWs.onmessage = null
                oldWs.onerror = null
                oldWs.onclose = null
                resolve()
              }
            })

            await closed
            this.#pendingReject = null
          } catch (err) {
            this.#ws.onmessage = null
            this.#ws.onerror = null
            this.#ws.onclose = null
            this.#pendingReject = null
            if (!this.#disposed) {
              controller.error(err)
            }
            break
          }
        }
      },
      cancel: () => {
        this.#disposed = true
        this.#ws.onmessage = null
        this.#ws.onerror = null
        this.#ws.onclose = null
        this.#ws.close()
        // awaitで停止中のPromiseを解放してwhileループを抜けさせる
        this.#pendingReject?.(new Error('disposed'))
        this.#pendingReject = null
      },
    })

    this.#streamReader = this.#stream.getReader()
  }

  static async connectWS(user: User): Promise<WebSocket> {
    const WS_PATH = `/ws/v1/chat?deviceId=${encodeURIComponent(user.deviceId)}`
    const url = await getSignedWsUrl(WS_PATH, user.accessToken)
    const ws = new WebSocket(url)
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve()
      ws.onerror = reject
    })
    return ws
  }

  static async connect(id: string, user: User): Promise<Thread> {
    return new Thread(id, user, await Thread.connectWS(user))
  }

  async uploadFile(opts: {
    file: File
    isImage?: boolean
  }): Promise<UploadedFile> {
    return this.#user.uploadFile({ file: opts.file, isImage: opts.isImage, threadId: this.id })
  }

  async *sendMessage(message: {
    mode: 'USER_INPUT' | 'DEEP_THINK' | 'AI_READ'
    contents: (
      | {
          type: 'text'
          text: string
        }
      | {
          type: 'file'
          file: UploadedFile
        }
    )[]
  }): AsyncGenerator<
    | { type: 'ack' }
    | { type: 'text-delta'; text: string }
    | { type: 'reasoning-start' }
    | { type: 'reasoning-delta'; text: string }
    | { type: 'done' }
    | { type: 'notification'; data: any }
    | { type: 'disconnected' }
    | { type: 'tool-call'; data: Array<{
        contentType: "TEXT" | "SUMMARY_TEXT";
        textData: { text: string; };
      }> }
    | { type: 'image-thumbnail'; url: string }
    | { type: 'image'; url: string }
    | { type: 'error';
        code: string;
        message: string;
        trace: {
          id: string;
          url: string;
        };
        threadId: string;
      }
  > {
    const messageId = crypto.randomUUID()
    this.#ws.send(
      JSON.stringify({
        message: {
          type: 'CONVERSATION',
          payload: {
            action: message.mode,
            data: {
              chatRequestType: message.mode,
              role: 'user',
              userId: this.#user.deviceId,
              threadId: this.id,
              messageId: messageId,
              language: 'ja',
              platform: 'WEB',
              timestamp: Date.now(),
              contents: message.contents.map((c) => {
                if (c.type === 'text') {
                  return {
                    contentType: 'TEXT',
                    textData: {
                      text: c.text,
                    },
                  }
                }
                if (c.type === 'file') {
                  if (c.file.isImage) return {
                    contentType: 'INPUT_IMAGE',
                    inputImageData: {
                      src: c.file.fileUrl,
                      resourceId: c.file.fileId,
                    },
                  }
                  else return {
                    contentType: 'INPUT_FILE',
                    inputFileData: {
                      src: c.file.fileUrl,
                      resourceId: c.file.fileId,
                      name: c.file.fileName,
                    },
                  }
                }
                throw new Error('Unsupported content type')
              }),
              retry: false,
              debug: false,
              timezoneString: 'Asia/Tokyo',
              countryCode: 'JP',
              city: 'Nerima',
              explicitSearch: 'AUTO',
            },
          },
          metadata: {
            messageId: messageId,
            timestamp: Date.now(),
          },
        },
      } satisfies ChatRequestMessage),
    )
    while (true) {
      const { value: chunk, done } = await this.#streamReader.read();
      if (done) { // 自動で再接続するのでこれが流れるはずは無い
        yield { type: 'disconnected' } as const
        return
      }
      if (chunk.webSocket.type === 'ACK') {
        yield { type: 'ack' } as const
      } else if (chunk.webSocket.type === 'CONVERSATION') {
        if (
          chunk.webSocket.payload.action === 'AI_ANSWER' ||
          chunk.webSocket.payload.action === 'EVENT'
        ) {
          if (
            chunk.webSocket.payload.data.chatResponseStatus === 'APPEND'
          ) {
            const contents = chunk.webSocket.payload.data.contents
            for (const content of contents) {
              if (content.contentType === 'TEXT') {
                if (chunk.webSocket.payload.action === 'EVENT') {
                  if (content.textData.text === '思考中...') {
                    yield {
                      type: 'reasoning-start',
                    } as const
                    continue
                  } else if (content.textData.text === '検索中...') {
                    // skip
                  }
                  continue
                }
                yield {
                  type: 'text-delta',
                  text: content.textData.text ?? '',
                } as const
              } else if (content.contentType === 'SUMMARY_TEXT') {
                yield {
                  type: 'reasoning-delta',
                  text: content.textData.text ?? '',
                } as const
              } else if (content.contentType === 'OUTPUT_IMAGE') {
                const img = content.outputImageData.imageGens[0]
                if(img) {
                  yield { type: 'image-thumbnail', url: img.thumbnail }
                  if(img.preview) yield { type: 'image', url: img.preview }
                }
              } else {
                console.warn(
                  '\n[Unsupported content type:',
                  content.contentType,
                  ']',
                  content,
                )
              }
            }
          } else if (
            chunk.webSocket.payload.data.chatResponseStatus === 'TOOL_CALL'
          ) {
            yield {
              type: 'tool-call',
              data: chunk.webSocket.payload.data.contents // FIXME: types
            }
          } else if (
            chunk.webSocket.payload.data.chatResponseStatus === 'DONE'
          ) {
            yield { type: 'done' } as const
            return
          } else {
            console.warn(
              'Received AI_ANSWER with unsupported chatResponseStatus:',
              chunk.webSocket.payload.data,
            )
          }
        } else {
          console.warn(
            'Received non-AI_ANSWER conversation message:',
            chunk.webSocket,
          )
        }
      } else if (chunk.webSocket.type === 'NOTIFICATION') {
        yield {
          type: 'notification',
          data: chunk.webSocket.payload.data,
        } as const
      } else if (chunk.webSocket.type === 'ERROR') {
        yield {
          type: 'error',
          ...chunk.webSocket.error,
        }
        return
      } else {
        console.warn('Received non-conversation message:', chunk.webSocket)
      }
    }
  }
  [Symbol.dispose]() {
    this.#stream.cancel()
    this.#ws.close()
  }
  close() {
    this[Symbol.dispose]()
  }
}
