from os import sep
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as pf

class OntimeLoader:
    def __init__(self, spark, filename: str):
        self.spark = spark
        self.filename = filename
    
    def load(self):

        df = self.spark.read.csv(self.filename, header=False, sep='|')
        
        return df
    
    @staticmethod
    def getOnlyForm1MktEqOpt(inputDataFrame: DataFrame) -> DataFrame:

        outputDataFrame = inputDataFrame.where(pf.col('_c69') == 'FORM-1')

        return outputDataFrame