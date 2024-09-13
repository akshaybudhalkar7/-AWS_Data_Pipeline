from os import path
from aws_cdk import (aws_s3, Stack, aws_lambda, aws_iam, Duration, aws_glue,
                     aws_s3_deployment as s3_deployment,
                     CfnOutput,
                     aws_logs as logs,
aws_s3_notifications as s3_notifications
                     )
from constructs import Construct
import os
from os import path
import json


class DemoStack(Stack):
    def __init__(self, scope: Construct, id:str, environment:None, **kwargs) -> None:
        super().__init__(scope,id,**kwargs)

        # # Create an S3 bucket for the Spotify files
        # spotify_bucket = aws_s3.Bucket(self, "SpotifyDataBucket")
        #
        #
        s3_bucket = aws_s3.Bucket(
            self,
            "%s-s3" % id,
            bucket_name="%s-s3" % id,
            versioned=True
        )
        #
        # # Upload the Glue job script to S3
        # deployment = s3_deployment.BucketDeployment(self, "DeployGlueJobScript",
        #                                             destination_bucket=glue_bucket,
        #     sources=[s3_deployment.Source.asset("glue_jobs")],  # Path to the local script directory
        # )
        #
        # # The location of the Glue script in S3
        # script_location = f"s3://{glue_bucket.bucket_name}/glue_script.py"
        #
        # # Glue Job IAM Role
        # glue_role = aws_iam.Role(self, "GlueJobRole",
        #     assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
        #     managed_policies=[
        #         aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
        #         aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
        #     ]
        # )
        #
        # # Grant the role access to the script in S3
        # glue_bucket.grant_read(glue_role)
        #
        # glue_job = aws_glue.CfnJob(self, "MyGlueJob",
        #                            name="my_glue_job",
        #                            role=glue_role.role_arn,
        #                            command=aws_glue.CfnJob.JobCommandProperty(
        #                                name="glueetl",
        #                                script_location=script_location,
        #                                python_version="3"
        #                            ),
        #                            default_arguments={
        #                                "--job-langauge": "python",
        #                                "--enable-metric":"true",
        #                                "--target_bucket":spotify_bucket.bucket_name
        #                            },
        #                            max_retries=2,
        #                            max_capacity=2.0,
        #                            description="ETL job",
        #                            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
        #                                max_concurrent_runs=1
        #                            ),
        #                            glue_version="3.0",  # Specify Glue version
        #
        #                            )
        #
        # # Output the name of the Glue job
        # CfnOutput(self, "GlueJobName",
        #     value=glue_job.ref,
        #     description="The name of the created Glue job"
        # )
        #
        # Create a Glue Database
        database = aws_glue.CfnDatabase(self, "GlueDB",
            catalog_id=self.account,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name="etl_data_pipline"
            )
        )

        # Create an IAM Role for Glue Crawler
        crawler_role = aws_iam.Role(self, "GlueCrawlerRole",
                                role_name="%s-role" % id,
                                assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
                                managed_policies=[
                                    aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                                    aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
                                ]
                                )

        # Create a Glue Crawler
        crawler = aws_glue.CfnCrawler(self, f"Crawler",
                role=crawler_role.role_arn,  # Replace with your IAM role ARN
                database_name=database.ref,
                targets=aws_glue.CfnCrawler.TargetsProperty(
                    s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
                        path= f"s3://{s3_bucket.bucket_name}"
                    )]
                ),
                configuration=None,
                schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE",
                    delete_behavior="DEPRECATE_IN_DATABASE"
                ),
                crawler_security_configuration=None,
                description="CSV Crawler"
            )
        #
        # # Attach inline policy to allow the role to access the S3 bucket
        # crawler_role.add_to_policy(
        #     aws_iam.PolicyStatement(
        #         actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        #         resources=[spotify_bucket.bucket_arn, f"{spotify_bucket.bucket_arn}/*"]
        #     )
        # )
        #
        # # List of files to create crawlers for
        # files = ["features_data/", "tracks_data/", "data_12_20/", "artist_data/", "albums_data/"]
        # #files = ["spotify_features_data_2023.csv", "spotify_tracks_data_2023.csv", "spotify_data_12_20_2023.csv", "spotify_artist_data_2023.csv", "spotify-albums_data_2023.csv"]
        # crawler_name_list = []
        # for file in files:
        #     # Create a Glue Crawler
        #     crawler = aws_glue.CfnCrawler(self, f"MyCrawler-{file}",
        #         role=crawler_role.role_arn,  # Replace with your IAM role ARN
        #         database_name=database.ref,
        #         targets=aws_glue.CfnCrawler.TargetsProperty(
        #             s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
        #                 path= f"s3://{spotify_bucket.bucket_name}/{file}"
        #             )]
        #         ),
        #         # table_prefix="spotify_",
        #         configuration=None,
        #         schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
        #             update_behavior="UPDATE_IN_DATABASE",
        #             delete_behavior="DEPRECATE_IN_DATABASE"
        #         ),
        #         crawler_security_configuration=None,
        #         description="Spotify Crawler"
        #     )
        #     crawler_name_list.append(crawler.ref)
        #
        # crawler_name_json = json.dumps(crawler_name_list)
        #
        # Create an IAM Role for the Lambda Function
        lambda_role = aws_iam.Role(self, "LambdaRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )
        #
        # Lambda Function to Trigger the Crawler
        lambda_function = aws_lambda.Function(self, "api_ingestion_lambda",
                                              runtime = aws_lambda.Runtime.PYTHON_3_11,
                                              function_name="api_ingestion_lambda",
                                              handler="app.handler",
                                              code=aws_lambda.Code.from_asset(os.path.join(os.path.dirname(__file__),'..','..', 'application')),
                                              role=lambda_role,
                                              timeout=Duration.minutes(5),
                                              log_retention=logs.RetentionDays.ONE_DAY,
                                              environment={
                                                  "s3_bucket":s3_bucket.bucket_name,
                                                  "crawler": crawler.ref
                                              }
                                           )
        #
        # # Grant the Lambda function permissions to access the S3 bucket
        # spotify_bucket.grant_read(lambda_function)
        #
        # # Create an S3 event notification to trigger the Lambda function
        # spotify_bucket.add_event_notification(
        #     aws_s3.EventType.OBJECT_CREATED,
        #     s3_notifications.LambdaDestination(lambda_function)
        # )



