import * as https from 'https'
import * as http from 'http'
import fetch from 'node-fetch'

class ElasticsearchBulkIndexer {
  constructor(username, password, url, chunkSize = 5000) {
    this.username = username
    this.password = password
    this.url = url
    this.agent = this.url.startsWith('https://') ? new https.Agent({ rejectUnauthorized: false }) : new http.Agent()
    this.basicAuthorization = Buffer.from(`${this.username}:${this.password}`).toString('base64')
    this.buffer = []
    this.chunkSize = chunkSize
  }

  async flush() {
    if (this.buffer.length === 0) {
      return
    }
    const requestBody = `${this.buffer.join('\n')}\n`
    const response = await fetch(`${this.url}/_bulk`, {
      method: 'POST',
      agent: this.agent,
      headers: {
        'Content-Type': 'application/x-ndjson',
        Authorization: `Basic ${this.basicAuthorization}`
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
