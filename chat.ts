import {
  createChatThread,
  fetchAnonymousToken,
  generateDeviceID,
  getSignedWsUrl,
  uploadFile,
} from './core/types'
import type { ChatRequestMessage, ChatResponseStream } from './core/chat-types'

const DEFAULT_AGENT_ID = '6812e64f9dfaf301f7000001'

export class User {
  readonly accessToken: string
  readonly deviceId: string
  constructor(deviceId: string, accessToken: string) {
    this.accessToken = accessToken
    this.deviceId = deviceId
  }
  static async create(): Promise<User> {
    const deviceId = generateDeviceID()
    const i = await fetchAnonymousToken(deviceId)
    return new User(deviceId, i.accessToken)
  }
  async createThread(opts?: { scenarioAgentId?: string; title?: string }): Promise<Thread> {
    const thread = await createChatThread(this.deviceId, this.accessToken, {
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
    const res = await uploadFile(this.deviceId, this.accessToken, {
      file: opts.file,
      threadId: opts.threadId ?? crypto.randomUUID(),
      isImage: opts.isImage,
    })
    return {
      fileId: res.fileId,
      fileUrl: res.fileUrl,
      fileName: res.originalFilename,
    }
  }
}
export interface UploadedFile {
  fileId: string
  fileUrl: string
  fileName: string
}
/* TODO: support multiple conversations */
export class Thread {
  readonly id: string
  #user: User
  #ws: WebSocket
  #streamReader: ReadableStreamDefaultReader<ChatResponseStream>
  constructor(id: string, user: User, ws: WebSocket) {
    this.id = id
    this.#user = user
    // 時々WebSocketが切れたら繋ぎ直す?
    this.#ws = ws
    this.#streamReader = new ReadableStream<ChatResponseStream>({
      start: (controller) => {
        // FIXME: 場合によっては初っ端のメッセージを取りこぼすけど知らない
        ws.onmessage = (event) => {
          controller.enqueue(JSON.parse(event.data) as ChatResponseStream)
        };

        ws.onerror = (error) => {
          controller.error(error)
        };

        ws.onclose = (event) => {
          Thread.connectWS(id, user).then(
            (newWS) => this.#ws = newWS
          ).catch(
            (reason) => controller.error(reason)
          )
        };
      },
      cancel: () => this.#ws.close(), // 一心同体
    }).getReader()
  }

  static async connectWS(id: string, user: User): Promise<WebSocket> {
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
    return new Thread(id, user, await Thread.connectWS(id, user))
  }

  async uploadFile(opts: {
    file: File
    isImage: boolean
  }) {
    this.#user.uploadFile({ file: opts.file, isImage: opts.isImage, threadId: this.id })
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
      | {
          type: 'image'
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
    | { type: 'image-thumbnail'; url: string }
    | { type: 'image'; url: string }
    | { type: 'tool-call'; data: Array<{
        contentType: "TEXT" | "SUMMARY_TEXT";
        textData: { text: string; };
      }> }
    | { type: 'disconnected' }
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
                  return {
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
              data: chunk.webSocket.payload.data.contents
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
      } else {
        console.warn('Received non-conversation message:', chunk.webSocket)
      }
    }
  }
  [Symbol.dispose]() {
    this.#ws.close()
  }
  close() {
    this[Symbol.dispose]()
  }
}
