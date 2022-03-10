import importlib

import re

from datetime import datetime

from typing import Any

from xml.etree.ElementTree import Element

import xml.etree.ElementTree as ET

 

#from gcs_jsonlogger.gcs_logger import GcsLogger

from jsonpath_ng.ext import parse

import json

 

#from layers.common.error.exceptions import ExceptionHandler

 

#LOGGER = GcsLogger("common.mapping.mapping_framework")

 

TARGET_ELEMENT = "targetElement"

TARGET_COLUMN_NAME = "targetColumnName"

TARGET_STRUCT_NAME = "targetStructName"

TARGET_DATA_TYPE = "targetDataType"

NUMBERING_PLAN = "numberingPlan"

PHONE = "phone"

CURRENCY = "currency"

ISO4217CODE = "iso4217Code"

USECOMMAS = "useCommas"

FORMAT = "format"

USD = "USD"

EUR = "EUR"

GBP = "GBP"

SEPARATOR = "separator"

ATTRIBUTES = "attributes"

ARTIFACT = "artifact"

CHILDREN = "children"

MAPPINGS = "mappings"

SOURCE = "source"

ACQUIRED_COLUMN_NAME = "acquiredColumnName"

ACQUIRED_COLUMN_KEY_NAME = "acquiredColumnKeyName"

TAXONOMY = "taxonomy"

PARTIAL_MATCH = "partialMatch"

RECORD = "record"

SPLIT = "split"

CONSTANT = "constantValue"

CONVERTER = "converter"

CONVERTER_FUNCTION = "converterFunction"

CONCAT = "concat"

TAXONOMY_NAME = "taxonomyName"

CUSTOM = "custom"

DATE_CONVERSION = "dateConversion"

ISO_DATE = "isoDate"

ROOT_ELEMENT = "root_element"

JSON_PATH = "jsonPath"

CONFIGURATION = "configuration"

INPUT_FORMAT = "inputFormat"

MULTIPLY = "multiply"

STRING_REPLACE = "stringReplace"

TARGET_COLUMN_STRUCT_NAME = "targetColumnStructName"

ARRAY = "array"

Record_file="XML Automation\\FBN Dallas\\record.json"
Mapping_file="XML Automation\\FBN Dallas\\mapping.json"
records = json.load(open("Record_file"))
#print(records)
mapping = json.load(open("Mapping_file"))

class MappingFramework:

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_mapping(record: Any, mapping_dict: dict, taxonomies: dict) -> dict:

        response = {}

        target_record = {}

        #LOGGER.info(f'handle_mapping: record={record}')

        array_of_structs = []

        root_element = MappingFramework.create_root_element(mapping_dict.get(ROOT_ELEMENT))

        config = MappingFramework.get_configuration(mapping_dict)

        input_format = MappingFramework.get_config_value(config, INPUT_FORMAT)

        # If the input data is xml, we want to convert it to an ElementTree once

        # if input_format == "xml":

        #     xml_record: ElementTree = ET.ElementTree(ET.fromstring(record))

        #     record = xml_record

        mappings = mapping_dict.get(MAPPINGS)

        for mapping in mappings:

            if TARGET_ELEMENT in mapping:

                target_element_name = mapping[TARGET_ELEMENT]

                if ATTRIBUTES in mapping:

                    target_element_attr = MappingFramework.handle_iso_date_conversion(record, mapping[ATTRIBUTES])

                    target_element_attr = target_element_attr if target_element_attr else mapping[ATTRIBUTES]

                    target_element = Element(target_element_name, target_element_attr)

                else:

                    target_element = Element(target_element_name)

                if SOURCE in mapping:

                    print(target_element)

                    MappingFramework.handle_source(mapping[SOURCE], target_element, record, taxonomies)

                    target_element_value = MappingFramework.handle_data_conversion(target_element.text,

                                                                                   mapping.get(TARGET_DATA_TYPE))

                    if FORMAT in mapping:

                        target_element_value = MappingFramework.format_data(target_element_value, mapping[FORMAT])

                    if target_element.text is not None and len(target_element.text) > 0:

                        target_element.text = str(target_element_value)

                        root_element.append(target_element)

                    if TARGET_COLUMN_NAME in mapping:

                        target_column_name = mapping[TARGET_COLUMN_NAME]

                        target_record[target_column_name] = target_element_value

                if CHILDREN in mapping:

                    if ARRAY == mapping.get(TARGET_DATA_TYPE):

 

                        if TARGET_COLUMN_STRUCT_NAME in mapping:

                            struct_dict = {mapping.get(TARGET_COLUMN_STRUCT_NAME): {}}

 

                        MappingFramework.handle_array(mapping.get(CHILDREN), target_element, record, taxonomies,

                                                      target_record, array_of_structs, mapping[TARGET_COLUMN_NAME], struct_dict, False)

                        array_of_structs.append(struct_dict)

                        if struct_dict.values():

                            target_record[mapping[TARGET_COLUMN_NAME]] = array_of_structs

 

                    else:

                        MappingFramework.handle_children(mapping.get(CHILDREN), target_element, record, taxonomies,

                                                         target_record)

                    if MappingFramework.has_children(target_element):

                        root_element.append(target_element)

        response[ARTIFACT] = root_element

        response[RECORD] = target_record

        return response

 

    @staticmethod

    def handle_data_conversion(value, _format):

        if not _format:

            return value

        if not value:

            return value

        try:

            if _format == 'int':

                return int(value)

            elif 'datetime' in _format:

                try:

                    _format = _format.split('~')[1]

                    if len(value) > 10:

                        return datetime.strptime(value, _format)

                    else:

                        formatted_date = datetime.strptime(value, _format)

                        return formatted_date.date()

                except ValueError as ve:

                    return value

            return value

        except Exception as ex:

            raise ex

 

    @staticmethod

    def format_data(value, format_dict):

        if 'phone' in format_dict:

            return MappingFramework.handle_phone_format(value, format_dict[PHONE][NUMBERING_PLAN],

                                                        format_dict[PHONE][SEPARATOR])

        elif 'currency' in format_dict:

            return MappingFramework.handle_currency_format(value, format_dict[CURRENCY][ISO4217CODE],

                                                           format_dict[CURRENCY][USECOMMAS])

 

    @staticmethod

    def handle_phone_format(phone_number, _format, split_char=' '):

        try:

            ph = re.sub('[^0-9]+', '', phone_number)

            formatted_phone_number = re.sub("(\d)(?=(\d{3})+(?!\d))", fr"\1{split_char}", "%s" % ph[:-1]) + ph[-1]

            return formatted_phone_number if _format == 'NANP' else '+' + formatted_phone_number.replace(split_char,

                                                                                                         ' ', 1)

        except Exception as ex:

            raise ex

 

    @staticmethod

    def handle_currency_format(currency_amount, _format, use_commas=True):

        try:

            if _format == USD:

                _format = "$"

            elif _format == EUR:

                _format = "€"

            elif _format == GBP:

                _format = "£"

            else:

                _format = ""

 

            currency_amount, decimal_count = (float(currency_amount), 2) if '.' in str(currency_amount) else (

                int(currency_amount), 0)

            prefix = '-' if currency_amount < 0 else ''

            comma = ',' if use_commas else ''

            formatted_amount = '%s%s%s' % (

                prefix, _format, '{:{}.{}f}'.format(abs(currency_amount), comma, decimal_count))

            return formatted_amount

        except Exception as ex:

            raise ex

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def get_configuration(mapping: dict):

        if "configuration" in mapping:

            return mapping.get("configuration")

        return None

 

    @staticmethod

    def get_config_value(config: dict, key: str):

        if config is not None:

            if key in config:

                return str(config.get(key)).lower()

        return None

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_children(children, parent_element: Element, record: dict, taxonomies: dict, target_record: dict):

        for child in children:

            if TARGET_ELEMENT in child:

                target_element_name = child[TARGET_ELEMENT]

                if ATTRIBUTES in child:

                    target_element_attr = MappingFramework.handle_iso_date_conversion(record, child[ATTRIBUTES])

                    target_element_attr = target_element_attr if target_element_attr else child[ATTRIBUTES]

                    target_element = Element(target_element_name, target_element_attr)

                else:

                    target_element = Element(target_element_name)

                if SOURCE in child:

                    # has_source = True

                    MappingFramework.handle_source(child[SOURCE], target_element, record, taxonomies)

                    target_element_value = MappingFramework.handle_data_conversion(target_element.text,

                                                                                   child.get(TARGET_DATA_TYPE))

                    if FORMAT in child:

                        target_element_value = MappingFramework.format_data(target_element_value, child[FORMAT])

                    if target_element.text is not None and len(target_element.text) > 0:

                        target_element.text = str(target_element_value)

                        parent_element.append(target_element)

                        # has_children = True

                    if TARGET_COLUMN_NAME in child:

                        target_column_name = child[TARGET_COLUMN_NAME]

                        target_record[target_column_name] = target_element_value

                if CHILDREN in child:

                    MappingFramework.handle_children(child[CHILDREN], target_element, record, taxonomies,

                                                     target_record)

                    if MappingFramework.has_children(target_element):

                        parent_element.append(target_element)

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def handle_array(children, parent_element: Element, record: dict, taxonomies: dict, target_record: dict, array: list, target_column_name, struct_dict: dict, nested_children: bool):

 

        for child in children:

            if TARGET_ELEMENT in child:

                target_element_name = child[TARGET_ELEMENT]

                if ATTRIBUTES in child:

                    target_element_attr = MappingFramework.handle_iso_date_conversion(record, child[ATTRIBUTES])

                    target_element_attr = target_element_attr if target_element_attr else child[ATTRIBUTES]

                    target_element = Element(target_element_name, target_element_attr)

                else:

                    target_element = Element(target_element_name)

                if TARGET_COLUMN_STRUCT_NAME in child:

                    if child[TARGET_COLUMN_STRUCT_NAME] not in struct_dict:

                        child_struct = {child[TARGET_COLUMN_STRUCT_NAME]: {}}

                        struct_dict[parent_element.tag].update(child_struct)

                if SOURCE in child:

                    # has_source = True

                    MappingFramework.handle_source(child[SOURCE], target_element, record, taxonomies)

                    target_element_value = MappingFramework.handle_data_conversion(target_element.text,

                                                                                   child.get(TARGET_DATA_TYPE))

                    if FORMAT in child:

                        target_element_value = MappingFramework.format_data(target_element_value, child[FORMAT])

                    if target_element.text is not None and len(target_element.text) > 0:

                        target_element.text = str(target_element_value)

                        parent_element.append(target_element)

                        # has_children = True

                    if TARGET_STRUCT_NAME in child:

                        target_struct_name = child[TARGET_STRUCT_NAME]

                        if target_element_value:

                            key = list(struct_dict.keys())[0]

                            # if key == parent_element.tag:

                            if key == parent_element.tag or not nested_children:

                                struct_dict[key][target_struct_name] = target_element_value

                            else:

                                struct_dict[key][parent_element.tag][target_struct_name] = target_element_value

                                # struct_dict['individual']['person'][target_struct_name] = target_element_value

                if CHILDREN in child:

                    nested_children = True

                    MappingFramework.handle_array(child[CHILDREN], target_element, record, taxonomies, target_record, array, target_column_name, struct_dict, nested_children)

 

                    if MappingFramework.has_children(target_element):

                        parent_element.append(target_element)

 

        # if struct_dict:

           # target_record[target_column_name] = array

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def has_children(element: Element) -> bool:

        if len(element.findall('*')) > 0:

            return True

        else:

            return False

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_source(source_dict, element: Element, record: dict, taxonomies: dict):

        if ACQUIRED_COLUMN_NAME in source_dict:

            MappingFramework.handle_acquired_column_name(record, element, source_dict[ACQUIRED_COLUMN_NAME])

        elif TAXONOMY in source_dict:

            MappingFramework.handle_taxonomy(source_dict[TAXONOMY], element, taxonomies, record)

        elif SPLIT in source_dict:

            MappingFramework.handle_split(record, element, source_dict[SPLIT])

        elif CONVERTER in source_dict:

            MappingFramework.handle_converter(record, element, source_dict[CONVERTER])

        elif CONCAT in source_dict:

            MappingFramework.handle_concat(record, element, source_dict[CONCAT], taxonomies)

        elif CONSTANT in source_dict:

            MappingFramework.handle_constant(element, source_dict[CONSTANT])

        elif CUSTOM in source_dict:

            MappingFramework.handle_custom_converter(record, element, source_dict[CUSTOM], taxonomies)

        elif JSON_PATH in source_dict:

            print(element)

            MappingFramework.handle_json_path_expression(record, element, source_dict[JSON_PATH])

        elif MULTIPLY in source_dict:

            MappingFramework.handle_multiply(record, element, source_dict[MULTIPLY])

        elif STRING_REPLACE in source_dict:

            MappingFramework.handle_string_replace(record, element, source_dict[STRING_REPLACE])

 

    @staticmethod

    def handle_constant(element: Element, constant: str):

        element.text = constant

 

    @staticmethod

    def handle_taxonomy(taxonomy_info: dict, element, taxonomies: dict, record: dict):

        element.text = MappingFramework.get_taxonomy_value(taxonomy_info, taxonomies, record)

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def get_taxonomy_value(taxonomy_info: dict, taxonomies: dict, record: dict) -> str:

        taxonomy_value = ''

        taxonomy_table_name = taxonomy_info[TAXONOMY_NAME]

        taxonomy_key_column_name = taxonomy_info[ACQUIRED_COLUMN_KEY_NAME]

        if taxonomy_key_column_name in record:

            if PARTIAL_MATCH in taxonomy_info:

                for key in taxonomies[taxonomy_table_name]:

                    if record[taxonomy_key_column_name].find(key) > 0:

                        taxonomy_value = taxonomies[taxonomy_table_name].get(key)

            else:

                taxonomy_value = taxonomies[taxonomy_table_name].get(str(record[taxonomy_key_column_name]).strip())

        return taxonomy_value

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_split(record: dict, element: Element, split: dict):

        key_column_name = split[ACQUIRED_COLUMN_NAME]

        split_char = split["splitChar"]

        index_pos = split["indexPos"]

        if key_column_name in record:

            element.text = MappingFramework.split(record[key_column_name], split_char, index_pos)

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def split(value: str, separator: chr, index: int):

        if value is not None and separator in value:

            split_list = value.split(separator)

            if index < len(split_list):

                return value.split(separator)[index]

        else:

            return ''

 

    @staticmethod

   #@ExceptionHandler.error_handler

    def handle_concat(record: dict, element: Element, concat_config: dict, taxonomies: dict):

        concat_value = ''

        for concat_item in concat_config:

            if TAXONOMY in concat_item:

                concat_value = concat_value + MappingFramework.get_taxonomy_value(concat_item[TAXONOMY], taxonomies,

                                                                                  record)

            elif ACQUIRED_COLUMN_NAME in concat_item:

                concat_value = concat_value + MappingFramework.get_acquired_column_value(record, concat_item[

                    ACQUIRED_COLUMN_NAME])

        element.text = concat_value

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_converter(record: dict, parent_element: Element, converter: dict):

        acquired_column_name = converter[ACQUIRED_COLUMN_NAME]

        converter_function = converter[CONVERTER_FUNCTION]

        if converter_function == "yesno" and acquired_column_name in record:

            parent_element.text = MappingFramework.yesno_converter(str(record[acquired_column_name]))

        elif converter_function == "streetCleanup" and acquired_column_name in record:

            parent_element.text = MappingFramework.street_converter(str(record[acquired_column_name]))

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def yesno_converter(status: str) -> str:

        temp_status = status.lower().strip()

        if temp_status == 'y':

            status = 'YES'

        elif temp_status == 'n':

            status = 'NO'

        else:

            status = 'NO'

        return status

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def street_converter(street: str) -> str:

        # add spaces around # and -

        street = re.sub(r"(#)", r" \1 ", street)

        # add spaces between certain dwelling types and alpha-numberic

        street = re.sub(r"\b(APT|UNIT|BSMT|BLDG?|DEPT|FRNT|HNGR|LBBY|LOT|LOWR|OFFICE|OFC|PIER|ROOM|RM|SPC|STOP|SUITE|TRLR|UPPR|SIDE|SLIP|REAR|KEY|STE|FL|PH|HWY)\s?(\W*|\w*)", r"\1 \2", street, 2)

        # cleanup multiple spaces added by previous regex's

        street = re.sub(' +', ' ', street).rstrip()

        return street

 

    @staticmethod

    #@ExceptionHandler.error_handler

    def handle_acquired_column_name(record: dict, element: Element, acquired_column_name):

        if acquired_column_name in record:

            element.text = MappingFramework.get_acquired_column_value(record, acquired_column_name)

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def handle_custom_converter(record: dict, element: Element, custom_config: dict, taxonomies: dict):

        module_name = custom_config['moduleName']

        class_name = custom_config['className']

        method_name = custom_config['methodName']

        custom_data = custom_config.get('customData')

        element.text = MappingFramework.call_custom_method(module_name, method_name, class_name, record, element,

                                                           taxonomies, custom_data)

 

    @staticmethod

  #  @ExceptionHandler.error_handler

    def call_custom_method(module_name, method_name, class_name, record: dict, element: Element, taxonomies: dict,

                           custom_data: str):

        module = importlib.import_module(module_name)

        clazz = getattr(module, class_name)

        func = getattr(clazz, method_name)

        if custom_data:

            record['custom_config_data'] = custom_data

        return func(record, element, taxonomies)

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def get_acquired_column_value(record, acquired_column_name):

        if acquired_column_name in record:

            return MappingFramework.handle_zero_value(str(record[acquired_column_name]).strip())

        else:

            return ''

 

    @staticmethod

    def handle_zero_value(value):

        if value == '0':

            return None

        else:

            return value

 

    @staticmethod

   # @ExceptionHandler.error_handler

    def handle_date_conversion(value):

        iso_date = datetime.strptime(value, '%m/%d/%Y').date()

        return iso_date

 

    @staticmethod

  #  @ExceptionHandler.error_handler

    def create_root_element(config: dict):

        root = Element("artifact")

        root.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')

        # need to make these as parameter in the configuration

        for k, v in config.items():

            root.set(k, v)

        return root

 

    @staticmethod

  #  @ExceptionHandler.error_handler

    def handle_json_path_expression(record: dict, element, json_path):

        json_path_list = []

        try:

            if isinstance(json_path, str):

                json_path_list.append(json_path)

            elif isinstance(json_path, list):

                json_path_list = json_path

 

            for path in json_path_list:

                jsonpath_expr = parse(path)

                get_match = jsonpath_expr.find(record)

                if len(get_match) > 0:

                    if get_match[0].value and get_match[0].value != 'None':

                        element.text = str(get_match[0].value)

                        break

                # else:

                #     element.text = None

        except Exception:

            #LOGGER.exception(f'Failed to convert record using json expression {json_path}', exc_info=True)

            element.text = None

            raise

 

    @staticmethod

  #.error_handler

    def handle_multiply(record: dict, element: Element, multiply: dict):

        acquired_column_name = multiply[ACQUIRED_COLUMN_NAME]

        if acquired_column_name in record:

            value = int(record[acquired_column_name])

            factor = int(multiply[CONSTANT])

            element.text = str(value * factor)

 

    @staticmethod

  #  @ExceptionHandler.error_handler

    def handle_string_replace(record: dict, element: Element, replace_string: dict):

        acquired_column_name = replace_string[ACQUIRED_COLUMN_NAME]

        if acquired_column_name in record:

            element.text = str(record[acquired_column_name].replace(

                replace_string['searchString'],

                replace_string['replaceString'])).strip()

 

    @staticmethod

   #.error_handler

    def handle_iso_date_conversion(record: dict, attr):

        if DATE_CONVERSION in attr:

            if ISO_DATE in attr[DATE_CONVERSION]:

                source_date = record[attr[DATE_CONVERSION][ISO_DATE]]

                conv_iso_date = MappingFramework.handle_date_conversion(source_date)

                return {ISO_DATE: str(conv_iso_date).replace('-', '')}

artifact = MappingFramework.handle_mapping(records,mapping,{})["artifact"]

print(artifact)

tree = ET.ElementTree(artifact)

tree.write('output1.xml')
