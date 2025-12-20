// 型定義
interface ApiResponse<T> {
  code: string
  message: string
  data: T
  meta?: {
    messageId?: string
    timestamp?: number
  }
}

interface AuthTokens {
  accessToken: string
  refreshToken: string
  idToken: string
  userType: string
}

// スレッド作成リクエストの型
interface CreateThreadRequest {
  scenarioAgentId: string // 使用するAIエージェントのID
  title?: string // スレッドのタイトル（任意）
  sourceLanguage?: string // ソース言語 (例: "ja")
  targetLanguage?: string // ターゲット言語 (例: "en")
}

// スレッド情報の型
interface ThreadData {
  id: string
  userId: string
  scenarioAgentId: string
  title: string
  createdAt: number
  updatedAt: number
}

export const generateDeviceID = () => {
  const n = Math.random().toString(36).substring(2, 8)
  const s = `${crypto.randomUUID()}-${n}`
  return s
}

const BASE_URL = 'https://ai.rakuten.co.jp' // Br.apiBasePath
const SECRET_KEY =
  '4f0465bfea7761a510dda451ff86a935bf0c8ed6fb37f80441509c64328788c8' // Zye()

export function generateNonce(): string {
  return crypto.randomUUID()
}

/** HMAC-SHA256 署名生成 (Yye相当) */
export async function generateSignature(
  message: string,
  secret: string,
): Promise<string> {
  const encoder = new TextEncoder()
  const keyData = encoder.encode(secret)
  const messageData = encoder.encode(message)

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )

  const signatureBuffer = await crypto.subtle.sign(
    'HMAC',
    cryptoKey,
    messageData,
  )
  const signatureArray = Array.from(new Uint8Array(signatureBuffer))

  // Base64URL エンコード
  return btoa(String.fromCharCode(...signatureArray))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '')
}

/** 署名用ヘッダーの組み立て (pNe相当) */
export async function getSignedHeaders(
  method: string,
  urlPath: string,
  params: Record<string, string> = {},
) {
  const timestamp = Math.floor(Date.now() / 1000).toString()
  const nonce = generateNonce()

  // パラメータをアルファベット順にソートして連結
  const sortedParams = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join('')

  // シグネチャ対象文字列: [METHOD][PATH][PARAMS][TIMESTAMP][NONCE]
  const rawString = `${method.toUpperCase()}${urlPath}${sortedParams}${timestamp}${nonce}`
  const signature = await generateSignature(rawString, SECRET_KEY)

  return {
    'X-Timestamp': timestamp,
    'X-Nonce': nonce,
    'X-Signature': signature,
  }
}

/** 匿名認証トークンの取得 */
export async function fetchAnonymousToken(
  DEVICE_ID: string,
): Promise<AuthTokens> {
  const endpoint = '/api/v2/auth/anonymous'
  const url = `${BASE_URL}${endpoint}`

  // 1. 署名ヘッダーの取得
  const signedHeaders = await getSignedHeaders('GET', endpoint)

  // 2. fetch の実行
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'X-Platform': 'WEB',
      'X-Country-Code': 'JP',
      'Device-ID': DEVICE_ID,
      ...signedHeaders,
    },
  })

  if (!response.ok) {
    throw new Error(`HTTP Error: ${response.status}`)
  }

  const result = (await response.json()) as ApiResponse<AuthTokens>

  // 3. 業務エラーチェック (on.handle相当)
  if (result.code !== '0') {
    throw new Error(result.message || 'Authentication Failed')
  }

  return result.data
}

const WS_BASE_URL = 'wss://companion.ai.rakuten.co.jp' // Br.wsBasePath

/** WebSocket 用の署名生成 (mNe相当) */
export async function getSignedWsUrl(
  path: string,
  accessToken: string,
): Promise<string> {
  const timestamp = Math.floor(Date.now() / 1000).toString()
  const nonce = generateNonce()
  const method = 'GET'

  // 1. ベースとなる URL オブジェクトを作成
  const wsUrl = new URL(`${WS_BASE_URL}${path}`)

  // 2. 認証トークンをパラメータに追加
  wsUrl.searchParams.set('accessToken', accessToken)
  wsUrl.searchParams.set('platform', 'WEB')

  // 3. 署名対象のパラメータを整理 (x- で始まるものを除外してソート)
  // ここでは既存の searchParams を元に組み立てる
  const params: Record<string, string> = {}
  wsUrl.searchParams.forEach((value, key) => {
    if (!key.toLowerCase().startsWith('x-')) {
      params[key] = value
    }
  })

  const sortedParamString = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join('')

  // 4. 署名対象文字列の組み立て: [METHOD][PATH][PARAMS][TIMESTAMP][NONCE]
  const rawString = `${method}${wsUrl.pathname}${sortedParamString}${timestamp}${nonce}`
  const signature = await generateSignature(rawString, SECRET_KEY)

  // 5. 署名用パラメータを URL に追加
  wsUrl.searchParams.set('x-timestamp', timestamp)
  wsUrl.searchParams.set('x-nonce', nonce)
  wsUrl.searchParams.set('x-signature', signature)

  return wsUrl.toString()
}
/**
 * 新しいチャットスレッドを作成する
 */
export async function createChatThread(
  deviceId: string,
  token: string,
  requestData: CreateThreadRequest,
): Promise<ThreadData> {
  const endpoint = '/api/v1/thread'
  const url = `${BASE_URL}${endpoint}`
  // 2. 署名ヘッダーの生成
  // POSTリクエストの場合、ボディは署名対象に含めず、
  // メソッド、パス、(URLクエリ)パラメータ、時間、ノンスで構成されるのが一般的です。
  const signedHeaders = await getSignedHeaders('POST', endpoint)

  // 3. fetch の実行
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      'X-Platform': 'WEB',
      'X-Country-Code': 'JP',
      'Device-ID': deviceId, // 以前定義した関数
      ...signedHeaders,
    },
    body: JSON.stringify(requestData),
  })

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`HTTP Error ${response.status}: ${errorText}`)
  }

  const result = (await response.json()) as ApiResponse<ThreadData>

  // 4. 業務エラーチェック
  if (result.code !== '0') {
    throw new Error(result.message || 'Failed to create thread')
  }

  return result.data
}

export async function uploadFile(
  deviceId: string,
  token: string,
  requestData: {
    file: File
    threadId: string
  },
) {
  const endpoint = '/api/v1/files/upload'
  const url = `${BASE_URL}${endpoint}`
  // 2. 署名ヘッダーの生成
  // POSTリクエストの場合、ボディは署名対象に含めず、
  // メソッド、パス、(URLクエリ)パラメータ、時間、ノンスで構成されるのが一般的です。
  const signedHeaders = await getSignedHeaders('POST', endpoint)

  const formData = new FormData()
  formData.append('file', requestData.file)
  const jsonFile = new File(
    [
      JSON.stringify({
        type: 'USER_DATA',
        agentId: '6812e64f9dfaf301f7000001',
        threadId: requestData.threadId,
      }),
    ],
    'blob',
    { type: 'application/json' },
  )
  formData.append('request', jsonFile)

  // 3. fetch の実行
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'X-Platform': 'WEB',
      'X-Country-Code': 'JP',
      'Device-ID': deviceId, // 以前定義した関数
      ...signedHeaders,
    },
    body: formData,
  })

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`HTTP Error ${response.status}: ${errorText}`)
  }

  const result = (await response.json()) as ApiResponse<{
    originalFilename: string
    objectKey: string
    fileId: string
    openaiFileId: string
    azureFileId: string
    bytes: number
    fileUrl: string
    id: string
  }>

  // 4. 業務エラーチェック
  if (result.code !== '0') {
    throw new Error(result.message || 'Failed to create thread')
  }

  return result.data
}
