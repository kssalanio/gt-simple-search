package simplesearch


import java.net.URL

import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.vector.{Extent, Feature, Geometry, MultiPolygon, MultiPolygonFeature, ProjectedExtent}
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff._
import geotrellis.spark.io.Intersects
import geotrellis.util._
import geotrellis.raster.MultibandTile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.FloatingLayoutScheme
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import simplesearch.Constants

object RasterIO {
  def readGeotiffFromFilepath(raster_path: String)
                             (implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
//    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)
//    val hdfs_url = new URL(raster_path)

    val input_rdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(raster_path)

    // Tiling layout to TILE_SIZE x TILE_SIZE grids
//    val (_, rasterMetaData) =
//      TileLayerMetadata.fromRdd[SpatialKey](input_rdd, FloatingLayoutScheme(Constants.TILE_SIZE))

    val (_, rasterMetaData): (Int, TileLayerMetadata[SpatialKey]) =
      TileLayerMetadata.fromRDD(input_rdd, FloatingLayoutScheme(Constants.TILE_SIZE))

    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      input_rdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(Constants.RDD_PARTS)

    val tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
//    val tiled_rdd_meta: MultibandTileLayerRDD[SpatialKey] =
      MultibandTileLayerRDD(tiled_rdd, rasterMetaData)

    // Prints out Spatial Keys
//    tiled_rdd_meta.foreach{ mbtl =>
//      val spatial_key = mbtl._1
//      println("Spatial Key: [" + spatial_key.row.toString + "," + spatial_key.col.toString+"]")
//    }
    return tiled_rdd_meta
  }
}
