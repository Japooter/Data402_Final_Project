from clean_import import *

# s3 = boto3.client('s3')
def test_incorrect_bucket_academy():
    with pytest.raises(Exception) as e_info:
        load_academy_data('bad-bucket-name', 'Academy/')

def test_incorrect_bucket_talent_json():
    with pytest.raises(Exception) as e_info:
        check_talent_json_files('where-are-the-jsons', 'Talent/')

def test_no_prefix_bucket_talent_json():
    actual = check_talent_json_files('data-402-final-project', 'Talentless/')
    expected = None
    assert actual == expected
    # with pytest.raises(Exception) as e_info:
    #     check_talent_json_files('where-are-the-jsons', 'Talent/')

def test_incorrect_bucket_talent_txt():
    with pytest.raises(Exception) as e_info:
        list_txt_files('where-are-the-jsons', 'Talent/')

def test_incorrect_bucket_talent_csv():
    with pytest.raises(Exception) as e_info:
        load_talent_csv_data('where-are-the-jsons', 'Talent/')


print(clean_whitespace('gherkins and pickles are similar  '))
def test_whitespace():
    actual = clean_whitespace('  gherkins and pickles are similar  ')
    expected = 'gherkins and pickles are similar'
    assert actual == expected

def test_month_clean():
    # actual = clean_month('Sept2025')
    # expected = 'September-2025'
    actual = clean_month('Oct2025')
    expected = 'Oct-2025'
    assert actual == expected

def test_number():
    actual = clean_phone_numbers('9023-244 234 (344)')
    expected = ('9023244234344')
    assert actual == expected

def test_comb_time():
    actual = combine_date_and_month('2323-23-23', 'September')
    expected = 'September'
    assert actual == expected

def test_dobs_conv():
    actual = dobs_to_datetime('29/05/2020')
    expected = datetime.datetime.strptime('29/05/2020', '%d/%m/%Y').date()
    assert actual == expected

def test_capitalisation():
    actual = capitalise('the grand pizza fight')
    expected = 'The Grand Pizza Fight'
    assert actual == expected

def test_wildcards():
    actual = remove_wildcards('H4ell&45350o,!')
    expected = 'Hello'
    assert actual == expected

def test_wrong_sql_info():
    with pytest.raises(Exception) as e_info:
        insert_into_sql('this', 'is', 'wrong')

def test_get_date():
    actual = get_date('Business_20_2027-02-11')
    expected = datetime.date(2027, 2, 11)
    assert actual == expected

def test_get_stream():
    actual = get_stream('Business_20_2019-02-11')
    expected = 'Business20'
    assert actual == expected

def test_get_category():
    actual = get_category('Business_20_2019-02-11')
    expected = 'Business'
    assert actual == expected


import pytest
from unittest.mock import MagicMock
from moto import mock_aws

# Mock s3 and list_all_objects
s3 = MagicMock()
list_all_objects = MagicMock()


@pytest.fixture
def test_s3_boto():
    s3 = boto3.client('s3', region_name='us-east-1')
    return s3


@mock_aws
def test_access_denied(test_s3_boto):
    with pytest.raises(Exception) as e_info:
        bucket = "testbucket"
        key = "testkey"
        body = "testing"
        test_s3_boto.create_bucket(Bucket=bucket)
        test_s3_boto.put_object(Bucket=bucket, Key=key, Body=body)
        ls_output = list_all_objects(Bucket=bucket, Prefix=key)
        assert len(ls_output) == 1
        assert ls_output[0] == 'testkey'
