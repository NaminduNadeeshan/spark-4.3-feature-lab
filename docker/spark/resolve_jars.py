import glob, os, shutil, sys
from pyspark.sql import SparkSession

packages, target, keep = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
spark = SparkSession.builder.master("local[1]").config("spark.jars.packages", packages).getOrCreate()
spark.stop()
os.makedirs(target, exist_ok=True)
for jar in glob.glob(os.path.expanduser("~/.ivy2*/jars/*.jar")):
    name = os.path.basename(jar)
    if any(k in name for k in keep):
        shutil.copy(jar, target)
        print("copied", name)
