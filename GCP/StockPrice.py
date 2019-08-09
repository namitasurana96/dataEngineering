import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'testingjob'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

def printdata(value):
    print (value[1])

p = beam.Pipeline(options=options)

(p | "ReadFromFile" >> beam.io.ReadFromText('gs://first_project1/Salary_Data.csv')
   | "print data" >> beam.ParDo(printdata)
)
p.run()
