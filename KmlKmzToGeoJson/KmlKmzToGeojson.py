import arcpy
import os,shutil

def clearExistingShapefiles(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        #print(filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def convertKMLKMZToGeoJson(file_path,filename):
    
    file_name = filename.split(".")[0]
    # Output directory for the converted layer
    output_path = r"D:\SHARE_SRIK\Geoprocessing\output_layer"
    os.makedirs(output_path, exist_ok=True)
    output_fc = os.path.join(r"D:\SHARE_SRIK\Geoprocessing\Shapefile", file_name+".shp")

    #Delete the existing shapefile
    folder = "D:\SHARE_SRIK\Geoprocessing\Shapefile"
    clearExistingShapefiles(folder)


    # Output GeoJSON directory
    geojson_output_dir = r"D:\SHARE_SRIK\Geoprocessing\GeoJson_Layer"
    os.makedirs(geojson_output_dir, exist_ok=True)
    output_geojson = os.path.join(geojson_output_dir, file_name+".geojson")

    try:
        # Step 1: Convert KML to Layer (which creates a file geodatabase)
        arcpy.KMLToLayer_conversion(file_path, output_path)
        # Step 2: Locate the output geodatabase
        gdb_path = os.path.join(output_path, file_name+".gdb")

        # Set the workspace to the geodatabase
        arcpy.env.workspace = gdb_path

        # List all datasets in the geodatabase
        datasets = arcpy.ListDatasets("*", "All")

        if not datasets:
            raise FileNotFoundError(f"No datasets found in the geodatabase at: {gdb_path}")

        print("Datasets in the geodatabase:")
        feature_classes=None
        for dataset in datasets:
            print(f"Dataset: {dataset}")
            # List feature classes in the dataset
            feature_classes = arcpy.ListFeatureClasses(feature_dataset=dataset)

            if not feature_classes:
                print(f"No feature classes found in dataset: {dataset}")
            else:
                print(f"Feature classes in dataset {dataset}:")
                for fc in feature_classes:
                    #print(f"Feature class: {fc}")
                    # Build full path to the feature class
                    feature_class_path = os.path.join(gdb_path, dataset, fc)

                    # Convert the feature class to a shapefile
                    arcpy.CopyFeatures_management(feature_class_path, output_fc)

                    # Convert the shapefile to GeoJSON
                    arcpy.FeaturesToJSON_conversion(output_fc, output_geojson)

                    print(f"Conversion complete. GeoJSON saved at: {output_geojson}")
        print(feature_classes)
    except Exception as err:
        print(err)
    
    
# Input KML file path
kmlkmz_folder = r"D:\SHARE_SRIK\Geoprocessing\kingdom_wide_schools_LayerTo"

for filename in os.listdir(kmlkmz_folder):
    file_path = os.path.join(kmlkmz_folder, filename)
    convertKMLKMZToGeoJson(file_path,filename)
