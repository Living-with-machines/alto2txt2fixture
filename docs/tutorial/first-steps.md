## Installing

The installation process should be fairly easy to take care of, using `poetry`:

```sh
$ poetry install
```

However, this is only the first step in the process. As the script works through the alto2txt collections, you will either need to choose the slower option — mounting them to your computer (using `blobfuse`) — or the faster option — downloading the required zip files from the Azure storage to your local hard drive. In the two following sections, both of those options are described.

## Connecting `alto2txt` to the program

### Downloading local copies of `alto2txt` on your computer

!!! attention "This option will take up a lot of hard drive space"
    As of the time of writing, downloading all of alto2txt’s metadata takes up about 185GB on your local drive. You do not have to download all of the collections or all of the zip files for each collection, as long as you are aware that the resulting fixtures will be limited in scope.

#### Step 1: Log in to Azure using Microsoft Azure Storage Explorer

[Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer/) (MASE) is a great and free tool for downloading content off Azure. Your first step is to download and install this product on your local computer.

Once you have opened MASE, you will need to sign into the appropriate Azure account.

#### Step 2: Download the `alto2txt` blob container to your hard drive

On your left-hand side, you should see a menu where you can navigate to the correct “blob container”: `Living with Machines` > `Storage Accounts` > `alto2txt` > `Blob Containers`:

![/docs/img/azure-storage.png](/img/azure-storage.png)

You will want to replicate _the same structure_ as the Blob Container itself in a folder on your hard drive:

![/docs/img/local-storage.png](/img/local-storage.png)

Once you have the structure set up, you are ready to download all of the files needed. For each of the blob containers, make sure that you _download the `metadata` directory only_ onto your computer:

![/docs/img/metadata-fulltext.png](/img/metadata-fulltext.png)

Select all of the files and press the download button:

![/docs/img/files-selected.png](/img/files-selected.png)

Make sure you save all the zip files inside the correct local folder:

![/docs/img/ensure-correct-folder.png](/img/ensure-correct-folder.png)

The “Activities” bar will now show you the progress and speed:

![/docs/img/activity-bar.png](/img/activity-bar.png)

### Mounting `alto2txt` on your computer

!!! attention "This option will only work on a Linux or UNIX computer"
    If you have a mac, your only option is the one below.

#### Step 1: Install BlobFuse

Follow [the instructions for installing BlobFuse](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux#install-blobfuse-v1) and the instructions for [how to prepare your drive for mounting](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux#prepare-for-mounting).

#### Step 2: Set up SAS tokens

Follow [the instructions for setting up access to your Azure storage account](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux#authorize-access-to-your-storage-account).

#### Step 3: Mount your blobs

TODO #3: Write this section.

_Note that you can also search on the internet for ideas on how to create local scripts to facilitate easier connection next time._
