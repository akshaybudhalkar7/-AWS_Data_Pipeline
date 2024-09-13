from aws_cdk import App, Environment

from cdk.cdk_stack import DemoStack

app = App()

DemoStack(
    app,
    "snowman-etl-pipline",
    environment="sbx",
    env = Environment(
        account = '441058194263',
        region = 'us-east-1'
    )
)

app.synth()