package com.prosper.docprocessor.google;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.mortbay.log.Log;

public class GoogleDriveOCRServiceTest {

	// Google Folder ID for "/Prosper/OCR/Checks/Benchmark_240" folder. Extracted from the browser URL
	public static final String folderId = "1sL-z7waNAxT1ZhFhPU6cu9XxUi1DuXeT";

	public static final String inputFileDir = "/Users/cyang/Documents/OCR/Checks/Benchmarking_240_Checks/";

	protected GoogleDriveOCRService service = new GoogleDriveOCRService();
	
	
	@Test
	public void testBatchConvert() throws IOException {
		List<Path> paths = Files.list(Paths.get(inputFileDir))
				.filter(s -> s.toString().endsWith(".jpg"))
				//.map(s -> s.toString())
				.collect(Collectors.toList());
		
		// Traverse each file and
		for(Path path: paths) {
			Log.info("Uploading and converting: {}", path.getFileName().toString());
			
			String fileId = service.uploadAndOCRFile(folderId, path);
			
			// Generate the output file name e.g. /path/myfile.jpg -> /path/myfile.pdf
			String outputFile = path.toString().replaceFirst("[.][^.]+$", ".txt");
			service.exportAndDownloadFile(fileId, outputFile);
		}
	}
	
}
