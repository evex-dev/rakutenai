import {
  createChatThread,
  fetchAnonymousToken,
  generateDeviceID,
  getSignedWsUrl,
  refreshAuthToken,
  uploadFile,
  type AuthTokens,
  createShare,
  getShare,
  type CreateShareResponse,
} from './core/types.ts'
import type { ChatRequestMessage, ChatResponseStream } from './core/chat-types.ts'

const DEFAULT_AGENT_ID = '6812e64f9dfaf301f7000001'

export class User {
  private accessToken: string
  private refreshToken: string
  private expiresAt: number
  readonly deviceId: string
  private refreshPromise: Promise<void> | null = null

  private constructor(deviceId: string, tokens: AuthTokens) {
    this.deviceId = deviceId
    this.accessToken = tokens.accessToken
    this.refreshToken = tokens.refreshToken
    this.expiresAt = tokens.expiresAt
  }

  static async create(): Promise<User> {
    const deviceId = generateDeviceID()
    const tokens = await fetchAnonymousToken(deviceId)
    return new User(deviceId, tokens)
  }

  // 有効なアクセストークンを返す（必要ならリフレッシュ）
  async getAccessToken(): Promise<string> {
    // 有効期限の60秒前を切っていたらリフレッシュ
    if (Date.now() >= this.expiresAt - 60_000) {
      await this.#refreshAccessToken()
    }
    return this.accessToken
  }

  async #refreshAccessToken(): Promise<void> {
    // 同時に複数のリフレッシュが走らないようにする
    if (this.refreshPromise) {
      return this.refreshPromise
    }

    this.refreshPromise = (async () => {
      try {
        const newTokens = await refreshAuthToken(
          this.deviceId,
          this.accessToken,
          this.refreshToken,
        )
        this.accessToken = newTokens.accessToken
        this.refreshToken = newTokens.refreshToken
        this.expiresAt = newTokens.expiresAt
      } finally {
        this.refreshPromise = null
      }
    })()

    return this.refreshPromise
  }

  async createThread(opts?: { scenarioAgentId?: string; title?: string }): Promise<Thread> {
    const token = await this.getAccessToken()
    const thread = await createChatThread(this.deviceId, token, {
      scenarioAgentId: opts?.scenarioAgentId ?? DEFAULT_AGENT_ID,
      title: opts?.title ?? '新しいスレッド',
    })
    const connected = await Thread.connect(thread.id, this)
    return connected
  }

  async uploadFile(opts: {
    file: File
    threadId?: string
    isImage?: boolean
  }): Promise<UploadedFile> {
    const token = await this.getAccessToken()
    const res = await uploadFile(this.deviceId, token, {
      file: opts.file,
      threadId: opts.threadId ?? crypto.randomUUID(),
      isImage: opts.isImage,
    })
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
  constructor(id: string, user: User, ws: WebSocket) {
    this.id = id
    this.#user = user
    this.#ws = ws

    this.#stream = new ReadableStream<ChatResponseStream>({
      start: async (controller) => {
        while (true) {
          try {
            // 現在のWSの終了を待機するPromiseを作成
            const closed = new Promise<void>((resolve, reject) => {
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
                try {
                  this.#ws = await Thread.connectWS(this.#user)
                  resolve()
                } catch (err) {
                  reject(err)
                }
              }
            })

            await closed
          } catch (err) {
            this.#ws.onerror = null
            this.#ws.onclose = null
            controller.error(err)
            break
          }
        }
      },
      cancel: () => {
        this.#ws.onerror = null
        this.#ws.onclose = null
        this.#ws.close() // 一心同体
      },
    })

    this.#streamReader = this.#stream.getReader()
  }

  static async connectWS(user: User): Promise<WebSocket> {
    const token = await user.getAccessToken()
    const WS_PATH = `/ws/v1/chat?deviceId=${encodeURIComponent(user.deviceId)}`
    const url = await getSignedWsUrl(WS_PATH, token)
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
    | { type: 'ack'; messageId: string }
    | { type: 'text-delta'; text: string }
    | { type: 'reasoning-start' }
    | { type: 'reasoning-delta'; text: string }
    | { type: 'done'; messageIds: string[] }
    | { type: 'notification'; data: any }
    | { type: 'disconnected' }
    | { type: 'tool-call'; data: Array<{
        contentType: "TEXT" | "SUMMARY_TEXT";
        textData: { text: string; };
      }> }
    | { type: 'tool-call-detail'; data: {
      name: string
      description: string
      groupId: string
      } }
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
    const userMessageId = crypto.randomUUID()
    const messageIds: string[] = [userMessageId]
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
              messageId: userMessageId,
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
            messageId: userMessageId,
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
        yield { type: 'ack', messageId: userMessageId } as const
      } else if (chunk.webSocket.type === 'CONVERSATION') {
        if (
          chunk.webSocket.payload.action === 'AI_ANSWER' ||
          chunk.webSocket.payload.action === 'EVENT'
        ) {
          if (
            chunk.webSocket.payload.data.chatResponseStatus === 'APPEND'
          ) {
            if ('progressEvent' in chunk.webSocket.payload.data) {
              yield { type: "tool-call-detail", data: chunk.webSocket.payload.data.progressEvent }
            } else {
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
            }
          } else if (
            chunk.webSocket.payload.data.chatResponseStatus === 'TOOL_CALL'
          ) {
            yield {
              type: 'tool-call',
              data: chunk.webSocket.payload.data.contents
            }
          } else if (
            chunk.webSocket.payload.data.chatResponseStatus === 'DONE'
          ) {
            // AIメッセージIDを収集（実際のレスポンスから取得）
            const aiMessageId = chunk.webSocket.payload.data.messageId
            if (aiMessageId) {
              messageIds.push(aiMessageId)
            }
            yield { type: 'done', messageIds: [...messageIds] } as const
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

  /** スレッドのメッセージを共有する */
  async createShare(messageIds: string[]): Promise<CreateShareResponse> {
    const token = await this.#user.getAccessToken()
    return createShare(this.#user.deviceId, token, {
      threadId: this.id,
      messageIds,
    })
  }

  /**
   * 共有されたスレッドから新しいスレッドを作成する（fork）
   * @param shareId 共有ID
   * @param user ユーザーインスタンス
   * @returns 新しく作成されたThreadインスタンス
   */
  static async fromShared(shareId: string, user: User): Promise<Thread> {
    const token = await user.getAccessToken()

    // 1. 共有データを取得してタイトルを取得
    const shareData = await getShare(user.deviceId, token, shareId)
    const originalTitle = shareData.title || 'Shared Thread'

    // 2. 共有リンクから新しいスレッドを作成（fork）
    const threadData = await createChatThread(user.deviceId, token, {
      scenarioAgentId: shareData.scenarioAgentId,
      title: `Continue: ${originalTitle}`,
      shareableLinkId: shareId,
      multipleThreadMode: true, // 常にtrue（暗黙的なfork）
    })

    // 3. WebSocketに接続してThreadインスタンスを作成
    return Thread.connect(threadData.id, user)
  }
}
