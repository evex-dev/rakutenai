import type {
  LanguageModelV2DataContent,
  LanguageModelV2,
  LanguageModelV2StreamPart,
} from '@ai-sdk/provider'
import { type Thread, User, type UploadedFile } from './chat'

export type RakutenAIModelId = 'ai-read' | 'deep-think' | 'normal'

export interface RakutenAIProviderOptions {
  user?: User
  thread?: Thread
}
export const rakutenAI = (
  modelId: RakutenAIModelId,
  opts?: {
    fetch: typeof fetch
  },
): LanguageModelV2 => {
  const fetch = opts?.fetch ?? globalThis.fetch.bind(globalThis)

  const cachedUploads = new WeakMap<
    Omit<LanguageModelV2DataContent, string>,
    UploadedFile
  >()

  return {
    provider: 'rakutenai',
    modelId: modelId,
    specificationVersion: 'v2',
    supportedUrls: {},
    async doGenerate(options) {
      throw new Error('RakutenAI only supports streaming responses')
    },
    async doStream(options) {
      if (options.prompt.length !== 1) {
        throw new Error('RakutenAI only supports single prompt')
      }
      const [message] = options.prompt
      if (!message) {
        throw new Error('Prompt message is empty')
      }
      if (message.role !== 'user') {
        throw new Error('RakutenAI only supports user role in prompt')
      }

      const providerOptions = options.providerOptions?.rakutenai as
        | RakutenAIProviderOptions
        | undefined
      const user = providerOptions?.user ?? (await User.create())
      const thread = providerOptions?.thread ?? (await user.createThread())

      const response = thread.sendMessage({
        mode: (
          {
            normal: 'USER_INPUT',
            'ai-read': 'AI_READ',
            'deep-think': 'DEEP_THINK',
          } satisfies Record<
            RakutenAIModelId,
            'AI_READ' | 'DEEP_THINK' | 'USER_INPUT'
          >
        )[modelId ?? 'normal'],
        contents: await Promise.all(
          message.content.map(async (part) => {
            if (part.type === 'text') {
              return { type: 'text', text: part.text }
            }
            // file
            const cached = cachedUploads.get(part.data)
            if (cached) {
              return { type: 'file', file: cached }
            }
            if (part.data instanceof URL) {
              throw new Error('URL file is not supported yet')
            }
            const upload = await user.uploadFile({
              file: new File([part.data], part.filename ?? 'unknown', {
                type: part.mediaType,
              }),
            })
            if (typeof part.data !== 'string') {
              cachedUploads.set(part.data, upload)
            }
            return { type: 'file', file: upload }
          }),
        ),
      })

      return {
        stream: new ReadableStream<LanguageModelV2StreamPart>({
          async start(controller) {
            let isTextStarted = false

            try {
              for await (const chunk of response) {
                if (chunk.type === 'ack') {
                  controller.enqueue({
                    type: 'stream-start',
                    warnings: [],
                  })
                } else if (chunk.type === 'notification') {
                  // ignore
                } else if (chunk.type === 'reasoning-delta') {
                  controller.enqueue({
                    type: 'reasoning-delta',
                    delta: chunk.text,
                    id: '',
                  })
                } else if (chunk.type === 'text-delta') {
                  if (!isTextStarted) {
                    controller.enqueue({
                      type: 'text-start',
                      id: '',
                    })
                    isTextStarted = true
                  }
                  controller.enqueue({
                    type: 'text-delta',
                    delta: chunk.text,
                    id: '',
                  })
                } else if (chunk.type === 'done') {
                  if (isTextStarted) {
                    controller.enqueue({
                      type: 'text-end',
                      id: '',
                    })
                  }
                  controller.enqueue({
                    type: 'finish',
                    finishReason: 'stop',
                    usage: {
                      inputTokens: 0,
                      outputTokens: 0,
                      totalTokens: 0,
                    },
                  })
                }
              }
              controller.close()
              thread.close()
            } catch (err) {
              controller.error(err)
            }
          },
        }),
      }
    },
  }
}
