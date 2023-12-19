import configparser


class ConfigReader:
    def readconfig(self, config_location):
        params = {}
        config = configparser.ConfigParser()
        config.read(config_location)
        params['input_file_location'] = config['INGEST_PARAMETERS']['INPUT_FILE_LOCATION']
        params['output_file_location'] = config['PERSIST_PARAMETERS']['OUTPUT_FILE_LOCATION']
        params['recipe_filter'] = config['TRANSFORM_PARAMETERS']['RECIPES']
        params['save_output_file_format'] = config['PERSIST_PARAMETERS']['SAVE_FILE_FORMAT']
        return params
