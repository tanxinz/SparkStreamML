package com.sibat.gongan.imp

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.unit.TimeValue

import com.sibat.gongan.util.ESConnectionPool
import com.sibat.gongan.util.EZJSONParser

trait ESQueryTrait extends StatusTrait with IPropertiesTrait{

  private def RecallResource[T](process: TransportClient => T):T = {
    val client = ESConnectionPool.Connection
      try{
        process(client)
      }
      finally{
        ESConnectionPool.Close(client)
      }
  }

  /*******************
    term query as k-v
  *******************/
  def ESTermQuery(index:String,types:String,col:String,query:String) = {
    def _subQuery(client:TransportClient) = {
      client.prepareSearch(index)
        .setTypes(types)
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(QueryBuilders.termQuery(col, query))                 // Query
        // .setPostFilter(QueryBuilders.rangeQuery("col").from("A").to("B"))     // Filter
        // .setFrom(0).setSize(60).setExplain(true)
        .get()
    }
    RecallResource(_subQuery)
  }

  def ESMatchAll(index:String,types:String,size:Int,from:Int) = {
     def _subQuery(client:TransportClient) = {
       client.prepareSearch(index)
         .setTypes(types)
         .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
         .setFrom(from)
         .setSize(size)
         .setQuery(QueryBuilders.matchAllQuery())                 // Query
         // .setPostFilter(QueryBuilders.rangeQuery("col").from("A").to("B"))     // Filter
         // .setFrom(0).setSize(60).setExplain(true)
         .get()
     }
     RecallResource(_subQuery)
  }

  def ESScrollStart(index:String,types:String,size:Int) = {
    def _subQuery(client:TransportClient) = {
      client.prepareSearch(index)
        .setTypes(types)
        .setSearchType(SearchType.SCAN)
        .setScroll(TimeValue.timeValueMinutes(1))
        .setSize(size)
        .get()
    }
    RecallResource(_subQuery)
  }

  def ESScroll(scrollId:String) = {
    def _subQuery(client:TransportClient) = {
      client.prepareSearchScroll(scrollId)
        .setScroll(TimeValue.timeValueMinutes(5))
        .get()
    }
    RecallResource(_subQuery)
  }

  /***
    get 指定id查询
  ****/
  def ESGet(index:String,types:String,id:String) = {
    val get = (client:TransportClient) => client.prepareGet(index, types, id)
        .setOperationThreaded(false)
        .get()
    RecallResource(get)
  }

  def ESDelete(index:String,types:String,id:String) = {
    val get = (client:TransportClient) => client.prepareDelete(index, types, id)
        .get()
    RecallResource(get)
  }

  def ESUpdate(index:String,types:String,id:String,builder:XContentBuilder) = {
    val update = (client:TransportClient) => client.prepareUpdate(index, types, id)
        .setDoc(builder)
        .get()
    RecallResource(update)
  }

  def ESUpsert(index:String,types:String,id:String,builder:XContentBuilder) = {
    val indexRequest:IndexRequest = new IndexRequest(index, types, id).source(builder)
    val updateRequest:UpdateRequest = new UpdateRequest(index, types, id)
        .doc(builder)
        .upsert(indexRequest)
    val upsert = (client:TransportClient) => client.update(updateRequest).get()
    RecallResource(upsert)
  }

  /***
    查找到对应的hit
  ****/
  def queryFound(esindex:String,estype:String,target:String,record:String):Option[String] = {
    val nmark = new EZJSONParser(
                  ESTermQuery(esindex,estype,target,record).toString
                    )
                      .getMap("hits")
                      .mark
      // make sure hit the id
      if (nmark.getMap("total").query == "0.0")None
      else {
        val hit = nmark.getMap("hits")
                      .getList(0)
                      .getMap("_source")
                      .mark
        if (hit.getMap(ESIMPORTANT).query.equals("yes"))
          Some(nmark.getMap("hits")
                    .getList(0)
                    .getMap("_id")
                    .query)
        else None
      }
    }

}
