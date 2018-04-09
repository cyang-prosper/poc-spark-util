package com.prosper.docprocessor.google;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.api.client.http.FileContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleDriveOCRService {
	
	protected Drive service = GoogleDriveConfiguration.getDriveService();
	
	/**
	 * Upload a local file to Google Drive and convert it to Google Doc and OCR at the same time
	 * 
	 * @param remoteFolderId
	 * @param localFile
	 * @return
	 * @throws IOException
	 */
	public String uploadAndOCRFile(String remoteFolderId, Path localFile) throws IOException {
		Objects.requireNonNull(remoteFolderId, "Google Drive's folder ID must not be null");
		Objects.requireNonNull(localFile, "Path to local file must not be null");
		
		// Metadata for the new file
		File fileMetadata = new File();
		fileMetadata.setName(localFile.getFileName().toString());
		fileMetadata.setParents(Collections.singletonList(remoteFolderId));
		fileMetadata.setMimeType("application/vnd.google-apps.document"); // Auto-convert to Google Doc

		// Local file's path
		String localFileFullPath = localFile.toString();
		java.io.File filePath = new java.io.File(localFileFullPath);
		String mediaType = null;
		if(localFileFullPath.endsWith("jpg") || localFileFullPath.endsWith("jpeg")) {
			mediaType = "image/jpeg";
		}
		else if(localFileFullPath.endsWith("png")) {
			mediaType = "image/png";
		}
		else if(localFileFullPath.endsWith("pdf")) {
			mediaType = "application/pdf";
		}
		else {
			throw new RuntimeException("Cannot determine Media type for: "+localFileFullPath);
		}
		
		FileContent mediaContent = new FileContent(mediaType, filePath);
		
		// Upload
		File file = service.files().create(fileMetadata, mediaContent)
			    .setFields("id")
			    .execute();
		
		log.debug("File ID: " + file.getId());
		return file.getId();
	}
	
	/**
	 * 
	 * 
	 * @param remoteFileId
	 * @throws IOException
	 */
	public void exportAndDownloadFile(String remoteFileId, String outputFile) throws IOException {
		Objects.requireNonNull(remoteFileId, "ID for the file in Google Drive must not be null");
		Objects.requireNonNull(outputFile, "Path to local output file must not be null");
		
		// Export and download the text version of the Google Doc
		//String outputFile = "/Users/cyang/Documents/OCR/Checks/Benchmarking_240_Checks/3725051 - 7852972.txt";
		OutputStream outputStream = new FileOutputStream(outputFile);
		service.files().export(remoteFileId, "text/plain")
		    .executeMediaAndDownloadTo(outputStream);
	}
}
