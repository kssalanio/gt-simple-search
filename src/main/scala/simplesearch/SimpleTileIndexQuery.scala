package simplesearch

import geotrellis.raster.MultibandTile
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark.{MultibandTileLayerRDD, SpatialKey}
import geotrellis.vector.{MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import org.apache.hadoop.fs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.Intersects
import geotrellis.util._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.FloatingLayoutScheme
import org.apache.spark.util.SizeEstimator

object SimpleTileIndexQuery {
  def queryGeoTiffWithShp(query_shp_path: String, src_gtiff_rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]])
                         (implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] ={
    """
      |Input: Query SHP, Metadata SHP, Raster GTIFF
      |Output: Clipped Raster GTIFF, Clipped Metadata SHP
    """.stripMargin
    val features = ShapeFileReader.readMultiPolygonFeatures(query_shp_path)
    val query_geom: MultiPolygon = features.head.geom

    val result_rdd: MultibandTileLayerRDD[SpatialKey] = src_gtiff_rdd.mask(query_geom)

    // This uses geotrellis.spark.filter, not spark.rdd.RDD.filter
    val result_rdd2 = src_gtiff_rdd.filter().where(Intersects(query_geom))

//    val result_rdd3 = src_gtiff_rdd.filter {
//      tile_ftr =>
//        tile_ftr._1.extent.intersects(query_geom)
////      case (sp_key, mb_tile) =>
////        sp_key.extent(src_gtiff_rdd.metadata.layout).intersects(query_geom)
//    }

    return result_rdd
  }

  def queryGeoTiffWithShpRDD(query_shp_rdd: RDD[MultiPolygonFeature[Map[String, Object]]], src_gtiff_rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]])
                         (implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] ={
    """
      |Input: Query SHP, Metadata SHP, Raster GTIFF
      |Output: Clipped Raster GTIFF, Clipped Metadata SHP
    """.stripMargin
    val qry_shp_BC = sc.broadcast(query_shp_rdd.map { mp_ft =>
      mp_ft.geom
    }.collect.toSet)

    val result_rdd = src_gtiff_rdd.filter {
      tile_ftr =>
      qry_shp_BC.value.map{ qry_ft =>
        tile_ftr._1.extent(src_gtiff_rdd.metadata.layout).intersects(qry_ft)
      }.foldLeft(true)(_ && _)
    }

    return MultibandTileLayerRDD(result_rdd, src_gtiff_rdd.metadata)
  }

//  def queryFeaturesWithShp(query_shp_path: String, src_features_rdd: RDD[MultiPolygonFeature[Map[String, Object]]])
//                          (implicit sc: SparkContext): RDD[MultiPolygonFeature[Map[String, Object]]] ={
//    val features = ShapeFileReader.readMultiPolygonFeatures(query_shp_path)
//    val query_geom: MultiPolygon = features.head.geom
//    val query_ft_rdd = sc.parallelize(features)
////    val result_rdd: RDD[MultiPolygonFeature[Map[String, Object]]] = src_features_rdd.filter{
////      ft =>
////        if( ft.geom.intersects(query_geom) ) return ft
////    }
//    val result_rdd_2: RDD[MultiPolygonFeature[Map[String, Object]]] = src_features_rdd.filter.where(Intersects(query_geom))
//    return result_rdd
//  }

  def createTiledInvertedIndex(tile_dir_path: String, run_rep: Int,
                               metadata_fts: RDD[MultiPolygonFeature[Map[String, Object]]], savefile_dirpath: String)
                              (implicit sc : SparkContext): Unit ={
    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)
    //val json_files = getListOfFiles(tile_json_dir_path,List[String]("json"))
    val json_files_rdd = sc.wholeTextFiles(tile_dir_path)
    json_files_rdd.flatMap {
      case (path, text) =>
        val (first_line, other_lines) = text.trim.split("\n").splitAt(1)
        val fl_tokens = first_line(0).filterNot(c => c  == '{' || c == '}' | c == '"').split(",").map {
          text_string =>
            text_string.filterNot(c => c == '"').split(":")
        }
        //        println("FL_TOKENS: ")
        //        pprint.pprintln(fl_tokens)

        val tile_code = fl_tokens(1)(1)
        val tile_dataset_uid = fl_tokens(2)(1)
        other_lines.flatMap{
          text_line =>
            text_line.trim.filterNot(c => c  == '{' || c == '}').split("\",\"").map{
              text_string =>
                text_string.filterNot(c => c  == '"').split(":")(0)
            }.map(keyword => (keyword, path, tile_code, tile_dataset_uid))

          /**
            * Should be:
            *
            * }.map(keyword => (keyword, keyword_recursive_list, path, tile_code, tile_dataset_uid))
            *
            * Where <keyword_recursive_list> is the list of keywords
            *   for nested dictionaries or maps. Defaults to <keyword>
            *   if it is not nested
            */

        }
    }.map {
      case (keyword, path, tile_code, tile_dataset_uid) => (keyword, (path, tile_code, tile_dataset_uid))
      //      case (keyword, path, tile_code, tile_dataset_uid) => ((keyword, path), (tile_code, tile_dataset_uid))
      //      case (keyword, path, tile_code, tile_dataset_uid) => (path, keyword)
    }.distinct
      .groupByKey
      .mapValues(iterator => iterator.mkString(", "))
      .saveAsTextFile(tile_dir_path+"/inverted_idx_"+run_rep)
  }

  def createFeatureInvertedIndex(metadata_fts: RDD[MultiPolygonFeature[Map[String, Object]]], savefile_dirpath: String)
                                (implicit sc : SparkContext): Unit ={
    metadata_fts.map{ mp_feature =>
      val mp_ft_geometry = mp_feature.geom
      val mp_ft_attributes = mp_feature.data

      // Create Inverted index from keywords, with the "Document" being the feature

    }
  }


  def searchMetaShp(query_map: Map[String,Object],metadata_shp : RDD[MultiPolygonFeature[Map[String, Object]]]): Unit = {

  }

  //  def simpleQueryMetadataWithShp(tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], meta_shp_path : String, output_json_path : String)
  //                                (implicit sc: SparkContext) {
  //    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)
  //    val metadata_fts: Seq[MultiPolygonFeature[Map[String, Object]]] = ShapeFileReader.readMultiPolygonFeatures(meta_shp_path)
  //    val metadata_fts_rdd: RDD[MultiPolygonFeature[Map[String, Object]]] = sc.parallelize(metadata_fts, Constants.RDD_PARTS)
  //
  //    // Get extents and combine to a multipolygon
  //    val mapTransform = tiled_rdd_meta.metadata.getComponent[LayoutDefinition].mapTransform
  //    val tile_extents_mp = MultiPolygon(tiled_rdd_meta.map {
  //      case (k, tile) =>
  //        val key = k.getComponent[SpatialKey]
  //        mapTransform(key).toPolygon()
  //    }.collect())
  //
  //    val json_serializer = Json(DefaultFormats)
  //
  //    val meta_json_string : String = metadata_fts_rdd.aggregate[String]("")(
  //      {(acc, cur_ft) =>
  //        if(cur_ft.intersects(tile_extents_mp)){
  //          val map_json = Json(DefaultFormats).write(cur_ft.data)
  //          acc + ",\n" + map_json
  //        }else{
  //          acc
  //        }
  //      },
  //      {(acc1, acc2) =>
  //        if(acc1.length > 0 && acc2.length > 0) {
  //          acc1 + ",\n" + acc2
  //        }else{
  //          acc1 + acc2
  //        }
  //      })
  //
  //    println("METADATA JSON:")
  //    pprint.pprintln(meta_json_string)
  //
  //    if(output_json_path contains "hdfs:"){
  //      //TODO:write HDFS textfile
  //      val hdfs_out = new BufferedOutputStream(hdfs.create(new fs.Path(output_json_path)))
  //      println(s"HDFS: Home Directory [${hdfs.getHomeDirectory}]")
  //      println(s"HDFS: Writing metadata json to [${output_json_path}]")
  //      val json_bytes = meta_json_string.getBytes
  //      println(s"HDFS: Writing bytes of length [${json_bytes.length}]")
  //      hdfs_out.write(json_bytes)
  //      hdfs_out.close()
  //    }
  //    else{
  //      new PrintWriter(
  //        output_json_path)
  //      {
  //        write(meta_json_string + "\n"); close }
  //    }
  //
  //  }
}
