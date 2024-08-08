import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials

class GoogleDriveService(object):
    """
    A class used to represent an GoogleDrive service for uploading files
    gdrive_credentials.json must be in same folder, if not
    ...

    Attributes
    ----------


    Methods
    -------
    upload_folder(dir:str, id_parent:str)
        Upload entire folder substructure into parent folder.
    search_folder(self, foldername:str, id_parent:str)
        Search for folder in parent and return its id.
    create_folder(self, foldername:str, id_parent:str)
        Create folder in parent and return its id.
    upload_file(self, filename:str, filepath:str, id_parent:str)
        Upload file in parent and return its id.
    """
    def __init__(self, credentials:str) -> None:
        """
        """
        self._SCOPES=['https://www.googleapis.com/auth/drive']
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials

        creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), self._SCOPES)
        self.service = build('drive', 'v3', credentials=creds)  
    
    def upload_folder(self, dir:str, id_parent:str) -> None:
        """
        Recreate entire folder structure and upload all files inside id_parent folder.

        Parameters
        ----------
        dir : str
            path of the folder to be uploaded.
            dir is omited from structure.
        id_parent : str
            parent folder in which the content will be uploaded.

        """
        dirs_dict = {
            dir:id_parent
        }
        for path, subdirs, files in os.walk(dir):
            print(f'Walking by: {path}')
            for subdir in subdirs:
                if subdir not in dirs_dict:
                    new_id_parent = dirs_dict.get(path)
                    print(f'Searching for folder {subdir} in parent {path}:{new_id_parent}')

                    id = self.search_folder(subdir, new_id_parent)
                    if id is None:
                        id = self.create_folder(subdir, new_id_parent)

                    dirs_dict[path+"/"+subdir] = id
                    print(f'Creating dict entry {path+"/"+subdir} in {path+"/"+subdir}:{id}')
            
            for file in files:
                id = self.search_file(filename=file, id_parent=dirs_dict.get(path))
                if id is None:
                    print(f'Uploading file: {path+"/"+file} in {path}:{dirs_dict.get(path)}')
                    self.upload_file(filename=file, filepath= path+"/"+file, id_parent=dirs_dict.get(path))
                else:
                    print(f'File: {path+"/"+file} in {path}:{dirs_dict.get(path)} already exists')


            print('\n')

    def path_exists(self, search:str, dir:str, id_parent:str) -> bool:
        """
        Walk by parent folder/subfolders and list every item.
        if path is found return True else False.

        Parameters
        ----------
        search: str
            path of the folder or file to search.
        dir : str
            local path of the folder that mimic the drive folder.
            dir is omited from structure.
        id_parent : str
            parent folder from Google Drive where data was uploaded.

        Returns
        -------
        bool
            True if path exist in Google Drive inside parent folder.
            False if not.
        """
        dirs_dict = {
            dir:id_parent
        }
        last_path = search.split('/')[0]
        path=''
        for path in search.split('/')[1:]:
            new_id_parent = dirs_dict.get(last_path)
            print(f'Searching for folder {path} in parent {last_path}:{new_id_parent}')
            if path not in dirs_dict:
                id = self.search_folder(path, new_id_parent)
                # If is not a folder verify if is file
                if id is None:
                    id = self.search_file(path, new_id_parent)
                # If is not a folder nor a file 
                if id is None:
                    return False
                dirs_dict[last_path+'/'+path] = id
            last_path = last_path+'/'+path

        return True


    def search_folder(self, foldername:str, id_parent:str) -> str:
        """
        Search for the foldername within the parent folder and returns its id.

        Parameters
        ----------
        foldername : str
            name of the folder to search.
        id_parent : str
            parent folder in which the search will be executed.

        Returns
        -------
        str
            folder id found in Google Drive.
        """
        try:
            drive_folder_id = None
            # page_token = None
            # pylint: disable=maybe-no-member
            response = (
                self.service.files()
                .list(
                    q=f"mimeType='application/vnd.google-apps.folder' and parents in '{id_parent}' and name = '{foldername}'",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=None,
                )
                .execute()
            )
            for file in response.get("files", []):
                print(f'Found folder: {file.get("name")}, {file.get("id")}')
                drive_folder_id = file.get("id") 

        except HttpError as error:
            print(f"An error occurred: {error}")
            drive_folder_id = None
            raise error
        
        return drive_folder_id   
            
    def create_folder(self, foldername:str, id_parent:str) -> str:
        """
        Create new folder inside id_parent folder with name foldername 

        Parameters
        ----------
        foldername : str
            folder name to be created.
        id_parent : str
            parent folder in which the new folder will be created.

        Returns
        -------
        str
            folder id created in Google Drive.
        """
        print(f'Creating {foldername} in {id_parent}')
        try:
            # create drive api client
            file_metadata = {
                "name": f"{foldername}",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [id_parent]
            }
            # pylint: disable=maybe-no-member
            file = self.service.files().create(body=file_metadata, fields="id").execute()
            print(f'Folder ID: "{file.get("id")}".')

        except HttpError as error:
            print(f"An error occurred: {error}")
            raise error
        
        return file.get("id")
        
    def upload_file(self, filename:str, filepath:str, id_parent:str) -> str:
        """
        Search for the foldername inside parent folder and return the id.

        Parameters
        ----------
        filename : str
            name of the file.
        filepath : str
            path of the file.
        id_parent : str
            parent folder in which the file will be executed.

        Returns
        -------
        str
            file id created in Google Drive.
        """
        try:
            file_metadata = {
                'name' : f'{filename}',
                'parents': [f'{id_parent}'],
            }
            media = MediaFileUpload(filepath, resumable=True)
            file = (
                self.service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            print(f'File ID: "{file.get("id")}".')
            return file.get("id")

        except HttpError as error:
            print(f"An error occurred: {error}")
            file = None
            raise error

    def search_file(self, filename:str, id_parent:str) -> str:
        """
        Search for the file within the parent folder and returns its id.

        Parameters
        ----------
        foldername : str
            name of the folder to search.
        id_parent : str
            parent folder in which the search will be executed.

        Returns
        -------
        str
            folder id found in Google Drive.
        """
        try:
            drive_folder_id = None
            # page_token = None
            # pylint: disable=maybe-no-member
            response = (
                self.service.files()
                .list(
                    q=f"parents in '{id_parent}' and name = '{filename}'",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=None,
                )
                .execute()
            )
            for file in response.get("files", []):
                print(f'Found file: {file.get("name")}, {file.get("id")}')
                drive_folder_id = file.get("id") 

        except HttpError as error:
            print(f"An error occurred: {error}")
            drive_folder_id = None
            raise error
        
        return drive_folder_id 
