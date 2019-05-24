package simplesearch;

import java.io.*;
import java.net.*;
import java.util.*;

import org.geotools.feature.*;
import org.geotools.data.*;
import org.geotools.data.shapefile.*;
import com.vividsolutions.jts.geom.*;

/**
 *  Description of the Class
 *
 * @author    steve.ansari
 */
public class ClipShapefile {

    /**
     *  Description of the Method
     *
     * @param  shpFileURL                 Description of the Parameter
     * @exception  IOException            Description of the Exception
     * @exception  MalformedURLException  Description of the Exception
     */
    public static void process(URL shpFileURL, File clipOutputShpFile)
            throws IOException, MalformedURLException, IllegalAttributeException {

        // Set up tile geometry using JTS library from:
        // http://www.vividsolutions.com/jts/JTSHome.htm
        Envelope envelope = new Envelope(-100.0, -80.0, 30.0, 40.0);
        GeometryFactory geomFactory = new GeometryFactory();
        Geometry tileGeometry = geomFactory.toGeometry(envelope);



        // Load shapefile
        ShapefileDataStore ds = new ShapefileDataStore(shpFileURL);
        FeatureSource fs = ds.getFeatureSource();
        FeatureCollection fc = fs.getFeatures();

        // Create the output shapefile
        ShapefileDataStore outStore = new ShapefileDataStore(clipOutputShpFile.toURL());
        outStore.createSchema(fs.getSchema());
        FeatureWriter outFeatureWriter = outStore.getFeatureWriter(outStore.getTypeNames()[0], Transaction.AUTO_COMMIT);

        // Process shapefile
        Iterator iterator = fc.iterator();
        try {

            Object[] att = null;
            int count = 0;
            while (iterator.hasNext()) {
                // Extract feature and geometry
                Feature feature = (Feature) iterator.next();
                Geometry featureGeometry = feature.getDefaultGeometry();

                if (featureGeometry.intersects(tileGeometry)) {

                    // Clip
                    Geometry newGeom = featureGeometry.intersection(tileGeometry);

                    // The original schema is expecting the Geometry type to be MultiPolygon,
                    // but the clip method sometimes returns the type Polygon.  If that is the
                    // case, convert to MultiPolygon
                    if (newGeom instanceof Polygon) {
                        newGeom = geomFactory.createMultiPolygon(new Polygon[] { (Polygon)newGeom });
                    }

                    // Adjust the feature to represent the clipped geometry with same attributes
                    feature.setDefaultGeometry(newGeom);


                    // Get the next, empty feature from the writer
                    Feature writeFeature = outFeatureWriter.next();
                    att = feature.getAttributes(att);
                    // Set the attributes of the new feature by copying the old, adjusted feature,
                    // which includes the geometry (which is an attribute)
                    for (int n=0; n<att.length; n++) {
                        writeFeature.setAttribute(n, att[n]);
                    }
                    outFeatureWriter.write();

                    count++;
                }
            }

            System.out.println("ORIGINAL NUMBER OF FEATURES: "+fc.size());
            System.out.println("FILTERED NUMBER OF FEATURES: "+count);
            outFeatureWriter.close();

        } finally {
            fc.close(iterator);
        }

    }


    /**
     *  The main program for the ClipShapefile class
     *
     * @param  args  The command line arguments
     */
    public static void main(String[] args) {

        try {

            URL url = new File("C:\\devel\\jnx\\trunk\\build\\shapefiles\\counties.shp").toURL();
            File outFile = new File("H:\\Nexrad_Viewer_Test\\GT_Examples\\counties-clip.shp");

            process(url, outFile);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }