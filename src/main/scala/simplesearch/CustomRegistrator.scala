package simplesearch

import java.util.{Comparator, TreeMap}
import java.util.concurrent.ConcurrentHashMap

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers._
import com.esotericsoftware.kryo.io.{Input, Output}
import geotrellis.spark.io.kryo.{GeometrySerializer, KryoRegistrator, XTreeMapSerializer}
import org.apache.spark.geomesa.GeoMesaSparkKryoRegistratorEndpoint
import org.apache.spark.rdd.RDD
import org.geotools.data.DataStore
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import collection.JavaConverters._
import scala.collection.mutable.Seq

import scala.collection.mutable.WrappedArray
import scala.util.hashing.MurmurHash3

class CustomRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // TreeMap serializaiton has a bug; we fix it here as we're stuck on low
    // Kryo versions due to Spark. Hack-tastic.
    kryo.register(classOf[TreeMap[_, _]], (new XTreeMapSerializer).asInstanceOf[com.esotericsoftware.kryo.Serializer[TreeMap[_, _]]])

    kryo.register(classOf[(_,_)])
    kryo.register(classOf[::[_]])
    kryo.register(classOf[geotrellis.raster.ByteArrayFiller])

    // CellTypes
    kryo.register(geotrellis.raster.BitCellType.getClass)                     // Bit
    kryo.register(geotrellis.raster.ByteCellType.getClass)                    // Byte
    kryo.register(geotrellis.raster.ByteConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.ByteUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.UByteCellType.getClass)                   // UByte
    kryo.register(geotrellis.raster.UByteConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.UByteUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.ShortCellType.getClass)                   // Short
    kryo.register(geotrellis.raster.ShortConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.ShortUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.UShortCellType.getClass)                  // UShort
    kryo.register(geotrellis.raster.UShortConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.UShortUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.IntCellType.getClass)                     // Int
    kryo.register(geotrellis.raster.IntConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.IntUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.FloatCellType.getClass)                   // Float
    kryo.register(geotrellis.raster.FloatConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.FloatUserDefinedNoDataCellType])
    kryo.register(geotrellis.raster.DoubleCellType.getClass)                  // Double
    kryo.register(geotrellis.raster.DoubleConstantNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.DoubleUserDefinedNoDataCellType])

    // ArrayTiles
    kryo.register(classOf[geotrellis.raster.BitArrayTile])                    // Bit
    kryo.register(classOf[geotrellis.raster.ByteArrayTile])                   // Byte
    kryo.register(classOf[geotrellis.raster.ByteRawArrayTile])
    kryo.register(classOf[geotrellis.raster.ByteConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.ByteUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.UByteArrayTile])                  // UByte
    kryo.register(classOf[geotrellis.raster.UByteRawArrayTile])
    kryo.register(classOf[geotrellis.raster.UByteConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.UByteUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.ShortArrayTile])                  // Short
    kryo.register(classOf[geotrellis.raster.ShortRawArrayTile])
    kryo.register(classOf[geotrellis.raster.ShortConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.ShortUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.UShortArrayTile])                 // UShort
    kryo.register(classOf[geotrellis.raster.UShortRawArrayTile])
    kryo.register(classOf[geotrellis.raster.UShortConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.UShortUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.IntArrayTile])                    // Int
    kryo.register(classOf[geotrellis.raster.IntRawArrayTile])
    kryo.register(classOf[geotrellis.raster.IntConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.IntUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.FloatArrayTile])                  // Float
    kryo.register(classOf[geotrellis.raster.FloatRawArrayTile])
    kryo.register(classOf[geotrellis.raster.FloatConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.FloatUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.DoubleArrayTile])                 // Double
    kryo.register(classOf[geotrellis.raster.DoubleRawArrayTile])
    kryo.register(classOf[geotrellis.raster.DoubleConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.DoubleUserDefinedNoDataArrayTile])

    kryo.register(classOf[Array[geotrellis.raster.Tile]])
    kryo.register(classOf[Array[geotrellis.raster.TileFeature[_,_]]])
    kryo.register(classOf[geotrellis.raster.Tile])
    kryo.register(classOf[geotrellis.raster.TileFeature[_,_]])

    kryo.register(classOf[geotrellis.raster.ArrayMultibandTile])
    kryo.register(classOf[geotrellis.raster.CompositeTile])
    kryo.register(classOf[geotrellis.raster.ConstantTile])
    kryo.register(classOf[geotrellis.raster.CroppedTile])
    kryo.register(classOf[geotrellis.raster.Raster[_]])
    kryo.register(classOf[geotrellis.raster.RasterExtent])
    kryo.register(classOf[geotrellis.raster.CellGrid])
    kryo.register(classOf[geotrellis.raster.CellSize])
    kryo.register(classOf[geotrellis.raster.GridBounds])
    kryo.register(classOf[geotrellis.raster.GridExtent])
    kryo.register(classOf[geotrellis.raster.mapalgebra.focal.TargetCell])
    kryo.register(geotrellis.raster.mapalgebra.focal.TargetCell.All.getClass)
    kryo.register(geotrellis.raster.mapalgebra.focal.TargetCell.Data.getClass)
    kryo.register(geotrellis.raster.mapalgebra.focal.TargetCell.NoData.getClass)

    kryo.register(classOf[geotrellis.spark.SpatialKey])
    kryo.register(classOf[geotrellis.spark.SpaceTimeKey])
    kryo.register(classOf[geotrellis.spark.io.index.rowmajor.RowMajorSpatialKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.zcurve.ZSpatialKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.zcurve.ZSpaceTimeKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.hilbert.HilbertSpatialKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.hilbert.HilbertSpaceTimeKeyIndex])
    kryo.register(classOf[geotrellis.vector.ProjectedExtent])
    kryo.register(classOf[geotrellis.vector.Extent])
    kryo.register(classOf[geotrellis.proj4.CRS])

    // UnmodifiableCollectionsSerializer.registerSerializers(kryo)
    kryo.register(geotrellis.spark.buffer.Direction.Center.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.Top.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.Bottom.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.Left.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.Right.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.TopLeft.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.TopRight.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.BottomLeft.getClass)
    kryo.register(geotrellis.spark.buffer.Direction.BottomRight.getClass)

    /* Exhaustive Registration */
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Coordinate]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.LinearRing]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Polygon]])
    kryo.register(classOf[Array[geotrellis.spark.io.avro.AvroRecordCodec[Any]]])
    kryo.register(classOf[Array[geotrellis.spark.SpaceTimeKey]])
    kryo.register(classOf[Array[geotrellis.spark.SpatialKey]])
    kryo.register(classOf[Array[geotrellis.vector.Feature[Any,Any]]])
    kryo.register(classOf[Array[geotrellis.vector.MultiPolygon]])
    kryo.register(classOf[Array[geotrellis.vector.Point]])
    kryo.register(classOf[Array[geotrellis.vector.Polygon]])
    kryo.register(classOf[Array[scala.collection.Seq[Any]]])
    kryo.register(classOf[Array[scala.Tuple2[Any, Any]]])
    kryo.register(classOf[Array[scala.Tuple3[Any, Any, Any]]])
    kryo.register(classOf[com.vividsolutions.jts.geom.Coordinate])
    kryo.register(classOf[com.vividsolutions.jts.geom.Envelope])
    kryo.register(classOf[com.vividsolutions.jts.geom.GeometryFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequence])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.LinearRing])
    kryo.register(classOf[com.vividsolutions.jts.geom.MultiPolygon])
    kryo.register(classOf[com.vividsolutions.jts.geom.Point])
    kryo.register(classOf[com.vividsolutions.jts.geom.Polygon])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel.Type])
    kryo.register(classOf[geotrellis.raster.histogram.FastMapHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.Histogram[Any]])
    kryo.register(classOf[geotrellis.raster.histogram.MutableHistogram[Any]])
    kryo.register(classOf[geotrellis.raster.histogram.StreamingHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.StreamingHistogram.DeltaCompare])
    kryo.register(classOf[geotrellis.raster.histogram.StreamingHistogram.Delta])
    kryo.register(classOf[geotrellis.raster.histogram.StreamingHistogram.Bucket])
    kryo.register(classOf[geotrellis.raster.density.KernelStamper])
    kryo.register(classOf[geotrellis.raster.summary.polygonal.MeanResult])
    kryo.register(classOf[geotrellis.raster.ProjectedRaster[Any]])
    kryo.register(classOf[geotrellis.raster.TileLayout])
    kryo.register(classOf[geotrellis.spark.TemporalProjectedExtent])
    kryo.register(classOf[geotrellis.spark.buffer.BufferSizes])
    kryo.register(classOf[geotrellis.spark.io.avro.AvroRecordCodec[Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.AvroUnionCodec[Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.codecs.KeyValueRecordCodec[Any,Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.codecs.TupleCodec[Any,Any]])
    kryo.register(classOf[geotrellis.spark.KeyBounds[Any]])
    kryo.register(classOf[geotrellis.spark.knn.KNearestRDD.Ord[Any]])
    kryo.register(classOf[geotrellis.vector.Feature[Any,Any]])
    kryo.register(classOf[geotrellis.vector.Geometry], new GeometrySerializer[geotrellis.vector.Geometry])
    kryo.register(classOf[geotrellis.vector.GeometryCollection])
    kryo.register(classOf[geotrellis.vector.Line], new GeometrySerializer[geotrellis.vector.Line])
    kryo.register(classOf[geotrellis.vector.MultiGeometry])
    kryo.register(classOf[geotrellis.vector.MultiLine], new GeometrySerializer[geotrellis.vector.MultiLine])
    kryo.register(classOf[geotrellis.vector.MultiPoint], new GeometrySerializer[geotrellis.vector.MultiPoint])
    kryo.register(classOf[geotrellis.vector.MultiPolygon], new GeometrySerializer[geotrellis.vector.MultiPolygon])
    kryo.register(classOf[geotrellis.vector.Point])
    kryo.register(classOf[geotrellis.vector.Polygon], new GeometrySerializer[geotrellis.vector.Polygon])
    kryo.register(classOf[geotrellis.vector.SpatialIndex[Any]])
    kryo.register(classOf[java.lang.Class[Any]])
    kryo.register(classOf[java.util.TreeMap[Any, Any]])
    kryo.register(classOf[java.util.HashMap[Any,Any]])
    kryo.register(classOf[java.util.HashSet[Any]])
    kryo.register(classOf[java.util.LinkedHashMap[Any,Any]])
    kryo.register(classOf[java.util.LinkedHashSet[Any]])
    kryo.register(classOf[org.apache.hadoop.io.BytesWritable])
    kryo.register(classOf[org.apache.hadoop.io.BigIntWritable])
    kryo.register(classOf[Array[org.apache.hadoop.io.BigIntWritable]])
    kryo.register(classOf[Array[org.apache.hadoop.io.BytesWritable]])
    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
    kryo.register(classOf[org.codehaus.jackson.node.IntNode])
    kryo.register(classOf[org.osgeo.proj4j.CoordinateReferenceSystem])
    kryo.register(classOf[org.osgeo.proj4j.datum.AxisOrder])
    kryo.register(classOf[org.osgeo.proj4j.datum.AxisOrder.Axis])
    kryo.register(classOf[org.osgeo.proj4j.datum.Datum])
    kryo.register(classOf[org.osgeo.proj4j.datum.Ellipsoid])
    kryo.register(classOf[org.osgeo.proj4j.datum.Grid])
    kryo.register(classOf[org.osgeo.proj4j.datum.Grid.ConversionTable])
    kryo.register(classOf[org.osgeo.proj4j.util.PolarCoordinate])
    kryo.register(classOf[org.osgeo.proj4j.util.FloatPolarCoordinate])
    kryo.register(classOf[org.osgeo.proj4j.util.IntPolarCoordinate])
    kryo.register(classOf[Array[org.osgeo.proj4j.util.FloatPolarCoordinate]])
    kryo.register(classOf[org.osgeo.proj4j.datum.PrimeMeridian])
    kryo.register(classOf[org.osgeo.proj4j.proj.LambertConformalConicProjection])
    kryo.register(classOf[org.osgeo.proj4j.proj.LongLatProjection])
    kryo.register(classOf[org.osgeo.proj4j.proj.TransverseMercatorProjection])
    kryo.register(classOf[org.osgeo.proj4j.proj.MercatorProjection])
    kryo.register(classOf[org.osgeo.proj4j.units.DegreeUnit])
    kryo.register(classOf[org.osgeo.proj4j.units.Unit])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofRef])
    kryo.register(classOf[scala.collection.Seq[Any]])
    kryo.register(classOf[scala.Tuple3[Any, Any, Any]])
    kryo.register(geotrellis.proj4.LatLng.getClass)
    kryo.register(geotrellis.spark.EmptyBounds.getClass)
    kryo.register(scala.collection.immutable.Nil.getClass)
    kryo.register(scala.math.Ordering.Double.getClass)
    kryo.register(scala.math.Ordering.Float.getClass)
    kryo.register(scala.math.Ordering.Int.getClass)
    kryo.register(scala.math.Ordering.Long.getClass)
    kryo.register(scala.None.getClass)

    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val cache = new ConcurrentHashMap[Int, SimpleFeatureSerializer]()

      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        val id = CustomRegistrator.putType(feature.getFeatureType)
        var serializer = cache.get(id)
        if (serializer == null) {
          serializer = new SimpleFeatureSerializer(feature.getFeatureType)
          cache.put(id, serializer)
        }
        out.writeInt(id, true)
        serializer.write(kryo, out, feature)
      }

      override def read(kryo: Kryo, in: Input, clazz: Class[SimpleFeature]): SimpleFeature = {
        val id = in.readInt(true)
        var serializer = cache.get(id)
        if (serializer == null) {
          serializer = new SimpleFeatureSerializer(CustomRegistrator.getType(id))
          cache.put(id, serializer)
        }
        serializer.read(kryo, in, clazz)
      }
    }
    kryo.setReferences(false)
    SimpleFeatureSerializers.simpleFeatureImpls.foreach(kryo.register(_, serializer, kryo.getNextRegistrationId))
  }
}

object CustomRegistrator {

  private val typeCache = new ConcurrentHashMap[Int, SimpleFeatureType]()

  GeoMesaSparkKryoRegistratorEndpoint.init()

  def identifier(sft: SimpleFeatureType): Int = math.abs(MurmurHash3.stringHash(CacheKeyGenerator.cacheKey(sft)))

  def putType(sft: SimpleFeatureType): Int = {
    val id = identifier(sft)
    if (typeCache.putIfAbsent(id, sft) == null) GeoMesaSparkKryoRegistratorEndpoint.Client.putType(id, sft)
    id
  }

  def putTypes(types: Seq[SimpleFeatureType]): Seq[Int] =
    types.map { sft =>
      val id = identifier(sft)
      typeCache.putIfAbsent(id, sft)
      id
    }

  def getType(id: Int): SimpleFeatureType =
    Option(typeCache.get(id)).orElse {
      fromSystemProperties(id) orElse GeoMesaSparkKryoRegistratorEndpoint.Client.getType(id) map {
        sft => typeCache.put(id, sft); sft
      }
    }.orNull

  def getTypes: Seq[SimpleFeatureType] = Seq(typeCache.values.asScala.toSeq: _*)

  def register(ds: DataStore): Unit = register(ds.getTypeNames.map(ds.getSchema))

  def register(sfts: Seq[SimpleFeatureType]): Unit = sfts.foreach(register)

  def register(sft: SimpleFeatureType): Unit = CustomRegistrator.putType(sft)

  def systemProperties(schemas: SimpleFeatureType*): scala.Seq[(String, String)] = {
    schemas.flatMap { sft =>
      val id = identifier(sft)
      val nameProp = (s"geomesa.types.$id.name", sft.getTypeName)
      val specProp = (s"geomesa.types.$id.spec", SimpleFeatureTypes.encodeType(sft))
      scala.collection.mutable.Seq(nameProp, specProp)
    }
  }

  private def fromSystemProperties(id: Int): Option[SimpleFeatureType] =
    for {
      name <- Option(GeoMesaSystemProperties.getProperty(s"geomesa.types.$id.name"))
      spec <- Option(GeoMesaSystemProperties.getProperty(s"geomesa.types.$id.spec"))
    } yield {
      SimpleFeatureTypes.createType(name, spec)
    }
}