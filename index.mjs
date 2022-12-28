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
  
  async query(sql) {
    const response = await fetch(`${this.url}/_sql`, {
      method: 'POST',
      agent: this.agent,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Basic ${this.basicAuthorization}`
      },
      body: JSON.stringify({ query: sql })
    })
    if (response.status !== 200) {
      throw new Error(`Invalid response status: ${response.status}`)
    }
    const responseBody = await response.json()
    return responseBody
  }

  async flush() {
    if (this.buffer.length === 0) {
      return
    }
    console.log(`${new Date().toISOString()}: flushing`)
    const response = await fetch(`${this.url}/_bulk`, {
      method: 'POST',
      agent: this.agent,
      headers: {
        'Content-Type': 'application/x-ndjson',
        Authorization: `Basic ${this.basicAuthorization}`
      },
      body: `${this.buffer.join('\n')}\n`
    })
    if (response.status !== 200) {
      throw new Error(`Invalid response status: ${response.status}`)
    }
    console.log(`${new Date().toISOString()}: flushed`)
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
