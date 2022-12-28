import * as https from 'https'
import fetch from 'node-fetch'

class ElasticsearchBulkIndexer {
  constructor(username, password, url, chunkSize = 5000) {
    this.username = username
    this.password = password
    this.url = url
    this.buffer = []
    this.chunkSize = chunkSize
  }

  async flush() {
    if (this.buffer.length === 0) {
      return
    }
    const requestBody = `${this.buffer.join('\n')}\n`
    const basicAuthorization = Buffer.from(`${this.username}:${this.password}`).toString('base64')
    const httpsAgent = new https.Agent({
      rejectUnauthorized: false
    })
    const response = await fetch(`${this.url}/_bulk`, {
      method: 'POST',
      agent: httpsAgent,
      headers: {
        'Content-Type': 'application/x-ndjson',
        Authorization: `Basic ${basicAuthorization}`
      },
      body: requestBody
    })
    if (response.status !== 200) {
      throw new Error(`Invalid response status: ${response.status}`)
    }
  }

  async index(indexName, messageId, message) {
    this.buffer.push(JSON.stringify({ "index" : { "_index" : indexName, "_id": messageId }}))
    this.buffer.push(JSON.stringify(message))
    if (this.buffer.length === this.chunkSize * 2) {
      await this.flush()
      this.buffer = []
    }
  }
}

export default ElasticsearchBulkIndexer
