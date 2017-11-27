package com.sibat.gongan.imp

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.client.transport.TransportClient

import com.sibat.gongan.util.ESConnectionPool
import com.sibat.gongan.util.EZJSONParser

trait ESQueryTrait extends StatusTrait {

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


  /***
    查找到对应的hit
  ****/
  def queryFound(esindex:String,estype:String,target:String,record:String):Option[Tuple2[String,String]] = {
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
        // if (!hit.getMap(ESIMPORTANT).query.equals("0"))
          Some((nmark.getMap("hits")
                    .getList(0)
                    .getMap("_id")
                    .query,
                hit.getMap("zdrystate").query))
        // else None
      }
    }

}
