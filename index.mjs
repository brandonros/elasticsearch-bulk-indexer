import * as https from 'https'
import fetch from 'node-fetch'

class ElasticsearchBulkIndexer {
  constructor() {
    this.buffer = []
    this.chunkSize = 5000
  }

  async flush() {
    if (this.buffer.length === 0) {
      return
    }
    const requestBody = `${this.buffer.join('\n')}\n`
    const basicAuthorization = Buffer.from(`${process.env.ELASTICSEARCH_USERNAME}:${process.env.ELASTICSEARCH_PASSWORD}`).toString('base64')
    const httpsAgent = new https.Agent({
      rejectUnauthorized: false
    })
    const response = await fetch(`${process.env.ELASTICSEARCH_URL}/_bulk`, {
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
