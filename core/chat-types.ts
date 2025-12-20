export type ChatResponseStream = {
  webSocket:
    | {
        type: 'ACK'
        metadata: {
          traceId: string
          timestamp: number
        }
        payload:
          | {
              action: 'MESSAGE_RECEIVED_ACK'
              data: {
                reqMessageId: string
                threadId: string
              }
            }
          | {
              action: 'USER_INPUT_ACK'
              data: {
                id: string
                deviceId: string
                deviceOffset: number
                threadId: string
                action: 'ADD'
                messageIds: string[]
                createAt: number
                updateAt: number
                sequence: number
              }
            }
      }
    | {
        type: 'CONVERSATION'
        metadata: {
          messageId: string
          traceId: string
          timestamp: number
        }
        payload: {
          action: 'AI_ANSWER' | 'EVENT'
          data:
            | {
                chatResponseType: 'AI_ANSWER' | 'EVENT'
                chatResponseStatus: 'APPEND' | 'TOOL_CALL'
                threadId: string
                messageId: string
                reqMessageId: string
                language: 'ja'
                contents: {
                  contentType: 'TEXT' | 'SUMMARY_TEXT'
                  textData: {
                    text: string
                  }
                }[]
                trace: {
                  id: string
                }
                chatLimitExceeded: false
                retry: false
                isStopped: false
                timestamp: number
              }
            | {
                chatResponseType: 'AI_ANSWER'
                chatResponseStatus: 'DONE'
                threadId: string
                messageId: string
                reqMessageId: string
                language: 'ja'
                trace: {
                  id: string
                  url: string
                }
                chatLimitExceeded: boolean
                retry: boolean
                isStopped: boolean
                timestamp: number
              }
        }
      }
    | {
        type: 'NOTIFICATION'
        payload: {
          action: 'THREAD_DATA'
          data: {
            id: string
            scenarioAgentId: string
            scenarioAgentName: {
              en: 'Rakuten AI'
            }
            scenarioAgentDescription: string
            scenarioAgentDisplayDescriptions: {
              en: string
              zh: string
              ja: string
            }
            scenarioAgentIcon: string
            scenarioAgentCreatedBy: 'Official'
            title: 'Chat with Rakuten AI'
            pin: false
            lastOperationTime: number
            createdAt: number
            updatedAt: number
            allowedInputTypes: ['TEXT_INPUT', 'IMAGE_INPUT', 'VOICE_INPUT']
            quickQuestionsMap: {
              ko: []
              ja: []
              en: []
              zh: []
            }
            threadMode: 'USER_INPUT'
          }
        }
      }
}

export type ChatRequestMessage = {
  message: {
    type: 'CONVERSATION'
    payload: {
      action: 'USER_INPUT' | 'DEEP_THINK' | 'AI_READ'
      data: {
        chatRequestType: 'USER_INPUT' | 'DEEP_THINK' | 'AI_READ'
        role: 'user'
        userId: string
        threadId: string
        messageId: string
        language: 'ja'
        platform: 'WEB'
        timestamp: number
        contents: (
          | { contentType: 'TEXT'; textData: { text: string } }
          | {
              contentType: 'INPUT_FILE'
              inputFileData: {
                src: string
                resourceId: string
                name: string
              }
            }
        )[]
        retry: false
        debug: false
        timezoneString: 'Asia/Tokyo'
        countryCode: 'JP'
        city: 'Nerima'
        explicitSearch: 'AUTO'
      }
    }
    metadata: {
      messageId: string
      timestamp: number
    }
  }
}
