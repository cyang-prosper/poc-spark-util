package com.prosper.sparkutil.download;

import org.junit.Test;

public class SupportSiteFileDownloaderTest {
	
	protected static final String dataSourceDir 		= "/Users/cyang/Documents/OCR/checks/Benchmarking_240_Checks/";
	protected static final String inputCSVFile 			= dataSourceDir+"Voided Check Labelled (243 checks).csv";
	protected static final String outputCSVFile			= dataSourceDir+"Voided Check Labelled Output (243 checks).csv";

	protected SupportSiteFileDownloader downloader = new SupportSiteFileDownloader();
	
	@Test
	public void testDownloadChecksForBenchmark() throws Exception {
		downloader.downloadMultipleFilesFromSupport(dataSourceDir, inputCSVFile, outputCSVFile);
	}
	
	
	@Test
	public void testDownloadRejectedChecks() throws Exception {
		
	}
}
