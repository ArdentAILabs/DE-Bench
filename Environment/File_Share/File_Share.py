import os
from azure.storage.fileshare import ShareServiceClient

from dotenv import load_dotenv
load_dotenv()

def create_file_share(session_id):

    with ShareServiceClient(
                account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.file.core.windows.net",
                credential=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
            ) as service_client:
                files_share_name = f"jobshare-files-{session_id}"
                deps_share_name = f"jobshare-dependencies-{session_id}"
                try:
                    files_share_client = service_client.create_share(
                        files_share_name
                    )
                    deps_share_client = service_client.create_share(
                        deps_share_name
                    )
                except Exception as e:
                    print(e)
                    # Clean up the job if share creation fails
                    raise Exception(f"Failed to create Azure File Share: {str(e)}")

                
                return files_share_name, deps_share_name