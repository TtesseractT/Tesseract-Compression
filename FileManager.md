File manager for cold storage.
I have a large HDD 12TB worth of projects and files that I use for cold storage.
There are a collection of duplicate files within the file system that I need to manage, I dont want to simply remove all duplicate files from the directory as that would break everything, what im suggesting is a compression algorithm that can compress all the files on the drive together, but with duplicate data, make a location of where the data is and keep one version for the distribution. aka when the end product is decompressed it uses the duplicate host file and duplicates it back to the other files linked orignally.

Its like having 100 duplicate files within 10 folders. when compressed it only needs to recognise 1 file from the duplicates and make a link to the location of the other duplicates, meaning you end up with 1 original duplicate and 100 links to where that file needs to be. 

So that when the file deflates the file system registeres that the original duplicate file marked in the file system needs to be copied across all the linked files. 

Analysis of files:
Duplicates should work on the basis of its contents/filename/size/type 
So that even if the files are the same, filename/size/type there needs to be a context aware match for it to be classed as a true duplicate. 

These files can even be zip/rar/ exc other compressed files. 

The system encoding and decoding the files needs to be threaded with the user able to allocate the number of cpu cores available to the task. 

The framework will be made in python. and have extensive testing for all aspects of the compression pipeline to make sure NO FILES GET DELETED BY ACCIDENT.

The system will need to be able to run on VERY VERY large amounts of data simultaniously. 

The output file will be called *.tesseract thats the name of the compression system `tesseract`.