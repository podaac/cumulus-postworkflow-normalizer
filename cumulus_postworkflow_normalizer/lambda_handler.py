# pylint: disable=R0022, R0912, R1702, R0914

"""lambda function used to nomalize postworkflow messages in aws lambda with cumulus"""

import fnmatch
import logging
import os
import re
import boto3
import botocore
from cumulus_logger import CumulusLogger
from cumulus_process import Process

REGION = os.environ.get("REGION", "us-west-2")
cumulus_logger = CumulusLogger('postworkflow-normalizer')
s3 = boto3.resource('s3', region_name=REGION)


class PostworkflowNormalizer(Process):
    """
    Image generation class to generate image for a granule file and upload to s3


    Attributes
    ----------
    logger: logger
        cumulus logger
    config: dictionary
        configuration from cumulus


    Methods
    -------

    """

    def __init__(self, *args, **kwargs):
        """class init function"""

        super().__init__(*args, **kwargs)
        self.logger = cumulus_logger

    def _get_role_for_bucket(self, bucket_name):
        """Get the appropriate role to assume for a given bucket.

        The role mappings should be configured in self.config['role_mappings'] as:
        {
            "exact-bucket-name": "arn:aws:iam::123456789012:role/MyRole",
            "bucket-prefix-*": "arn:aws:iam::123456789012:role/PrefixRole",
            "*bucket-suffix": "arn:aws:iam::123456789012:role/SuffixRole",
            "bucket-*pattern*": "arn:aws:iam::123456789012:role/PatternRole",
            "regex-pattern": "arn:aws:iam::123456789012:role/RegexRole"
        }

        Supports exact matches, wildcard patterns, and regular expression matching.

        Parameters
        ----------
        bucket_name: str
            Name of the S3 bucket

        Returns
        -------
        str or None
            Role ARN to assume, or None if no role is needed
        """
        role_mappings = self.config.get('role_mappings', {})

        # Check for exact bucket match first (fastest)
        if bucket_name in role_mappings:
            return role_mappings[bucket_name]

        # Check pattern matches
        for pattern, role in role_mappings.items():
            # Skip exact matches (already checked above)
            if pattern == bucket_name:
                continue

            # Handle regex patterns
            if self._is_regex_pattern(pattern):
                try:
                    if re.match(pattern, bucket_name):
                        return role
                except re.error as ex:
                    self.logger.warning(f"Invalid regex pattern '{pattern}': {ex}")
                    continue
            # Handle simple wildcard patterns
            elif pattern.endswith('*'):
                prefix = pattern[:-1]
                if bucket_name.startswith(prefix):
                    return role
            elif pattern.startswith('*'):
                suffix = pattern[1:]
                if bucket_name.endswith(suffix):
                    return role
            elif '*' in pattern:
                # Handle complex wildcard patterns
                if fnmatch.fnmatch(bucket_name, pattern):
                    return role

        return None

    def _is_regex_pattern(self, pattern):
        """Check if a pattern is a regex pattern for optimization.

        Parameters
        ----------
        pattern: str
            Pattern to check

        Returns
        -------
        bool
            True if pattern is a regex pattern

        Notes
        -----
        Detects regex patterns by checking for common regex indicators:
        ^, $, (, |, [, \\
        """
        # Quick checks for common regex indicators
        return (pattern.startswith('^') or
                pattern.endswith('$') or
                '(' in pattern or
                '|' in pattern or
                '[' in pattern or
                '\\' in pattern)

    def _assume_role(self, role_arn):
        """Assume an IAM role and return credentials.

        Parameters
        ----------
        role_arn: str
            ARN of the role to assume

        Returns
        -------
        dict
            Credentials dictionary with access_key, secret_key, and token

        Raises
        ------
        Exception
            If role assumption fails
        """
        try:
            sts_client = boto3.client('sts')
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName='ImageGeneratorSession'
            )

            credentials = response['Credentials']
            return {
                'aws_access_key_id': credentials['AccessKeyId'],
                'aws_secret_access_key': credentials['SecretAccessKey'],
                'aws_session_token': credentials['SessionToken']
            }
        except Exception as ex:
            self.logger.error(f"Error assuming role {role_arn}: {ex}", exc_info=True)
            raise

    def check_file_exists(self, bucket, key):
        """Check if an s3 file exists

        Returns
        ----------
        bool
            True if file exists, False if file doesn't exist
        """

        if bucket is None or key is None:
            return False

        role_arn = self._get_role_for_bucket(bucket)

        try:
            if role_arn:
                self.logger.info(f"Assuming role {role_arn} for bucket {bucket}")
                credentials = self._assume_role(role_arn)

                # Create a new S3 client with assumed role credentials
                s3_client = boto3.resource(
                    's3',
                    aws_access_key_id=credentials['aws_access_key_id'],
                    aws_secret_access_key=credentials['aws_secret_access_key'],
                    aws_session_token=credentials['aws_session_token']
                )

                s3_client.Object(bucket, key).load()

            else:
                s3.Object(bucket, key).load()

        except botocore.exceptions.ClientError as ex:
            self.logger.error(f"Error downloading file from S3: {ex}", exc_info=True)
            return False

        return True

    def process(self):
        """Main process to normalize cumulus message for postworkflows

        Returns
        ----------
        dict
            Payload that is returned to the cma which is a dictionary with list of granules
        """
        # list of extension to remove from files list
        extension_exclude_list = ('.png', '.cmr.json', '.dmrpp', '.fp', '.md5', '.bin')

        collection_files = self.config.get('collection').get('files')
        # default of a lambda is 512 megabytes
        lambda_ephemeral_storage = self.config.get('lambda_ephemeral_storage', 536870912)
        # 50 mb smaller so we don't use up all temp space
        smaller_ephemeral_storage = lambda_ephemeral_storage - 52428800
        data_regex = [file.get('regex') for file in collection_files if file.get('type') == 'data']
        max_data_file_size = 0

        for granule in self.input.get('granules', []):
            new_files = []

            for file in granule.get('files', []):
                file_name = file.get('fileName')
                file_type = file.get('type')
                file_size = file.get('size', 0)
                bucket = file.get('bucket')
                key = file.get('key')

                # Skip files with excluded extensions
                if file_name.endswith(extension_exclude_list):
                    continue

                # Check if file is of type 'data' or matches any regex pattern
                if file_type == 'data' or any(re.search(regex, file_name) for regex in data_regex):
                    if file_type != 'data':
                        file['type'] = 'data'
                    if self.check_file_exists(bucket, key):
                        new_files.append(file)
                        max_data_file_size = max(max_data_file_size, file_size)

            if len(new_files) == 0:
                raise ValueError('There are 0 identified data files')

            if 0 < max_data_file_size < smaller_ephemeral_storage:
                self.input['ecs_lambda'] = "lambda"
            else:
                self.input['ecs_lambda'] = "ecs"

            granule['files'] = new_files

            if 'cmrConceptId' not in granule:
                cmr_link = granule.get('cmrLink')
                if cmr_link:
                    granule['cmrConceptId'] = cmr_link.split('/')[-1].split('.')[0]

        return self.input


def handler(event, context):
    """handler that gets called by aws lambda

    Parameters
    ----------
    event: dictionary
        event from a lambda call
    context: dictionary
        context from a lambda call

    Returns
    ----------
        string
            A CMA json message
    """

    levels = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warn': logging.WARNING,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
    }
    logging_level = os.environ.get('LOGGING_LEVEL', 'info')
    cumulus_logger.logger.level = levels.get(logging_level, 'info')
    cumulus_logger.setMetadata(event, context)
    result = PostworkflowNormalizer.cumulus_handler(event, context=context)

    ecs_lambda = result['payload'].pop('ecs_lambda', None)
    if ecs_lambda is not None:
        result['meta']['collection']['meta']['workflowChoice']['ecs_lambda'] = ecs_lambda

    return result


if __name__ == "__main__":
    PostworkflowNormalizer.cli()
