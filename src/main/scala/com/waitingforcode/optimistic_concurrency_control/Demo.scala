package com.waitingforcode.optimistic_concurrency_control

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType


object Demo extends App {

  val credentialsProvider =new BasicCredentialsProvider()
  val credentials = new UsernamePasswordCredentials("elastic", "changeme")
  credentialsProvider.setCredentials(AuthScope.ANY, credentials)

  val client = new RestHighLevelClient(
    RestClient.builder(new HttpHost("localhost", 9200, "http"),
      new HttpHost("localhost", 9201, "http")).setHttpClientConfigCallback(new HttpClientConfigCallback() {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        httpClientBuilder
          .setDefaultCredentialsProvider(credentialsProvider)
      }
    })
  )

  val indexName = "test_letters"
  val createIndexRequest = new CreateIndexRequest(indexName)
  createIndexRequest.source(
    """
      |{
      |"settings": {
      |  "number_of_shards": 1,
      |  "number_of_replicas": 0
      |},
      |"mappings": {
      |  "properties": {
      |   "letter": {"type": "text"}
      |  }
      |}
      |}
    """.stripMargin, XContentType.JSON
  )
  val result = client.indices().create(createIndexRequest, RequestOptions.DEFAULT)

  // Indexing part
  val sequenceNumbers = Seq(
    None, Some(0), Some(1), Some(2), Some(5)
  )
  sequenceNumbers.foreach(maybeNumber => {
    val request = new IndexRequest(indexName)
      .id(s"test_a")
    maybeNumber.map(sequenceNumber => {
      request.setIfSeqNo(sequenceNumber)
      request.setIfPrimaryTerm(1)
    })
    val jsonString =
      s"""
         |{"letter": "AA${maybeNumber}"}
      """.stripMargin
    request.source(jsonString, XContentType.JSON)

    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    println(s"indexResponse=${indexResponse.status()}")
    println(s"sequence number=${indexResponse.getSeqNo}, primary term=${indexResponse.getPrimaryTerm}")
  })

}
