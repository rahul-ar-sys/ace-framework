import boto3

class RealS3Backend:
    def __init__(self, region: str):
        self.s3 = boto3.client("s3", region_name=region)

    def put_object(self, **kwargs):
        return self.s3.put_object(**kwargs)

    def get_object(self, **kwargs):
        return self.s3.get_object(**kwargs)

    def list_objects(self, **kwargs):
        return self.s3.list_objects_v2(**kwargs)

    def delete_object(self, **kwargs):
        return self.s3.delete_object(**kwargs)
