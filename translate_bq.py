import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

import os

# Hyper parameters for pipeline input and output.
project_id = 'data-analytics-project-234508'
dataset_name = 'kodansha_otakad'
table_name_in = 'unique_titles'
table_name_out = 'unique_titles_en'
table_spec_in = project_id+':'+dataset_name+'.'+table_name_in
table_spec_out = project_id+':'+dataset_name+'.'+table_name_out
# Text column to be translated in the input table.
text_column_in = 'td_title'
# Text column with translated text in the output table.
text_column_out = 'td_title_en'
# Schema for output table.
table_schema_out = 'td_title_en:STRING, td_title:STRING'

# Create a service account in your Google Cloud and download json credential.
# Replace the file name with your json file.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './data-analytics-project-234508-aa307b1c4b32.json'

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the translate pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Define pipeline options.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    p = beam.Pipeline(options=pipeline_options)

    def translate_text(text_dict, target="en"):
        """Translates text into the target language. Target must be an ISO 639-1 language code.
        See https://g.co/cloud/translate/v2/translate-reference#supported_languages
        
        Args:
          text_dict: Dictionary format input.
        """
        import six
        from google.cloud import translate_v2 as translate
    
        text = text_dict[text_column_in]
    
        translate_client = translate.Client()

        if isinstance(text, six.binary_type):
            text = text.decode("utf-8")

        # Text can also be a sequence of strings, in which case this method
        # will return a sequence of results for each text.
        result = translate_client.translate(text, target_language=target)
    
        result_str = result['translatedText']
        
        # Construct dict matching output table schema table_schema_out.
        return {text_column_out: result_str,
                text_column_in: text}

    # Debug. Test translate_text fn.
    text_dict = {}
    text_dict[text_column_in] = '寿司は美味しです'
    print(translate_text(text_dict))
   
    translate_jp2en = (
        p
        | 'Read table from BQ' >> beam.io.ReadFromBigQuery(table=table_spec_in)
        
        # Debug.
        # | 'Create dict' >> beam.Create([
        #       {
        #         'td_title': '魚も美味しいです'
        #       },
        #       {
        #         'td_title': '寿司は美味しいです'
        #       },
        #   ])
        
        # Each row is a dictionary where the keys are the BigQuery columns
        | 'Translating' >> beam.Map(translate_text)
    )
    
    # Debug. Print translated jp texts.
    # translate_jp2en | 'Print' >> beam.Map(print)
    
    # Write translated data back to a new table in BigQuery.
    translate_jp2en | 'Write back to BQ' >> beam.io.WriteToBigQuery(
            table_spec_out,
            schema=table_schema_out, 
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()