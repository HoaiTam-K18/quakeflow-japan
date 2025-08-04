from dagster import asset
@asset
def my_first_asset(context):
    
    context.log.info("This is my first asset")
    return 1