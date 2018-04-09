package com.prosper.sparkutil.download;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.opencsv.CSVReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SupportSiteFileDownloader {


	protected RestTemplate restTemplate = new RestTemplate();

	/**
	 * Download documents from Spark using CSV file as input of support site URLs
	 * 
	 * @throws Exception
	 */
	public void downloadMultipleFilesFromSupport(String dataSourceDir, String inputCSVFile, String outputCSVFile) throws Exception {
		// Input CSV file
		Reader reader = Files.newBufferedReader(Paths.get(inputCSVFile));
		CSVReader csvReader = new CSVReader(reader, ',', '\"');

		// Read the header row
		String[] headerRow = csvReader.readNext();
		Objects.requireNonNull(headerRow, "Header row cannot be null");

		String[] row = null;
		while ((row = csvReader.readNext()) != null) {
			String docId = row[0];
			String listingId = row[2];
			String fileType = row[7];
			String supportLink = row[11];

			// Prepare the output file path and check if the file already exists.
			String outputFile = dataSourceDir+docId+" - "+listingId+"."+fileType;
			Path outputPath = Paths.get(outputFile);
			if(Files.exists(outputPath)) {
				continue;
			}

			byte[] fileContent = downloadDoc(supportLink);

			if(fileContent==null) {
				log.warn("Failed to download {}", docId);
				continue;
			}

			Files.write(outputPath, fileContent);
		}

	}
	
	/**
	 * Download the binary content of the file using authorizaion cookie
	 * 
	 * @param supportLink
	 * @return
	 */
	protected byte[] downloadDoc(String supportLink) {
		HttpHeaders requestHeaders = new HttpHeaders();
		requestHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM));
		requestHeaders.add("Cookie", "__cfduid=d589d6dd8d1d8769f15c12275a883c4511509740688; optimizelyEndUserId=oeu1509740689230r0.5333342836270527; prosper_info_dev=ED=ygfWxb93QIxQXTI31Wsm6kGHwaByY3M+lRIn/bMyudyezrli83utXfQDc2yg0xweq9MVOKhOhTkAc0jiR6l/uI9Nqt4JVfD6WLb6jJyOe8CuGNKvqQbcaRU0jge4llZ6IhehByBpxQs2SXVHLa1w6me7OcDUfgA4XQp/KSdeWSc=; prosperRole=EMPLOYEE|FPA1|Shiva - Admin; s_fid=0A5CCE5F56068DB1-2C0887EB3B6AA235; optimizelySegments=%7B%223012200113%22%3A%22gc%22%2C%223034030089%22%3A%22false%22%2C%223036260040%22%3A%22referral%22%2C%223038280385%22%3A%22none%22%2C%226724540713%22%3A%22true%22%7D; _ga=GA1.2.397916522.1510345234; __zlcmid=jQgt2wYojuX2TW; prosperDomain=.prosper.com; prosperRole=NA; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%2221b63096-6ee2-4d83-ae7a-d755155ff1ab%22; IRF_80=%7Bvisits%3A1%2Cuser%3A%7Btime%3A1516400535416%2Cref%3A%22direct%22%2Cpv%3A1%2Ccap%3A%7B%7D%2Cv%3A%7B%7D%7D%2Cvisit%3A%7Btime%3A1516400535416%2Cref%3A%22direct%22%2Cpv%3A1%2Ccap%3A%7B270%3A1%7D%2Cv%3A%7B%7D%7D%2Clp%3A%22https%3A%2F%2Fwww.prosper.com%2Fdm%22%2Cdebug%3A0%2Ca%3A1516400535416%2Cd%3A%22prosper.com%22%7D; prosper_info=ED=ygfWxb93QIxQXTI31Wsm6kGHwaByY3M+lRIn/bMyudyAF2geMfyULwpm9zZle2iyOQ4o7yg4fBb4/cfTzxV9oVms/tWpvB5lgzX6KSVbrRukjEu9yg14eiMkwZe53ezlbEkiyBJtUH0IjGCBnOwpRI5Pze1R198OQPWvAulCLQ8=; prosper_info=ED=ygfWxb93QIxQXTI31Wsm6kGHwaByY3M+lRIn/bMyudyAF2geMfyULxjRYMX8vDjB01xECdp0d6Q4RHwPZBdHykdJjpNCBKPeIDGasKSdrLo0W8ta0j/u+bE03gbuy4bub1d/P/4Zu9zz9ZeVyhSvOu6F1zzOl1IRxsyzNdxr0ew=; ASP.NET_SessionId=fu4jjzuszcbzvuv0ocmpqmsg; optimizelyBuckets=%7B%228507532421%22%3A%228507812023%22%2C%228459947131%22%3A%228463881030%22%2C%226205183185%22%3A%226223381472%22%2C%227028950041%22%3A%227035950012%22%2C%228456791500%22%3A%228467150511%22%2C%228021162188%22%3A%228007053477%22%2C%228286466659%22%3A%228286683044%22%2C%226652400754%22%3A%226679130105%22%2C%226753422206%22%3A%226777521686%22%2C%226257300820%22%3A%226252493100%22%2C%228264021535%22%3A%228270682306%22%2C%228789704651%22%3A%228785576344%22%2C%229265144912%22%3A%229264264842%22%2C%227661802391%22%3A%227678380396%22%2C%227744910626%22%3A%227743183132%22%2C%228549505071%22%3A%228542053057%22%2C%226241982039%22%3A%226255090860%22%2C%228202773339%22%3A%228205295195%22%2C%228378290641%22%3A%228383182002%22%2C%228427721704%22%3A%228423925350%22%2C%2210188853222%22%3A%2210192486573%22%7D; ltmprsst=!55TBfS2zwzB5yGjWgbzFZrcqo5FknFHqV4hUgMI9nzxkL+K2m5KYoPu2VG+Iis/SUfpslQcMCN0G/w==; s_cc=true; AMCV_1E30776A524450F20A490D44%40AdobeOrg=283337926%7CMCIDTS%7C17613%7CMCMID%7C74989842384507346703182629509161412563%7CMCAAMLH-1522182614%7C9%7CMCAAMB-1522345307%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCAID%7CNONE; prosper-abp=MjgwNixjeWFuZ0Bwcm9zcGVyLmNvbSxDaHJpc3RvcGhlcixZYW5nLEVNUExPWUVFfEZQQTF8U2hpdmEgLSBBZG1pbg==; CircleOneSupportSite=8E637B536925600BFA5C19576FAE37986A4DA02D74B2878B40940FD18F1A77F1543A328B74E25BC6D97AE8C10394E2CDCDA8D24F11AADA656150EC59E3BA28C824C33DA67D889BBCA8C55B5786BE2C1BE0D25373F201C56EC1BCB17CB05ADFC0B828584675CA3669251BB19CDD892B215300CE7FAB0F313C8BC1D39FFCB8EA16EACB29CD64B319D508531820109837E04EBF7C47A68FD86ED741DD9304ABC1182ACEB7884F7AF39952A9C3E48FB1F8DE191C8517; prosperApiToken=DEFA5641CE0F5D46951899C2DA55A0670EE89FF63A52DD1963A9EE2B0D12E0F86E4F6A7E2FCDBE1247555E0F4BCBF8B6A09C541EAF07534F00B3B9604C6528F0002752EDFE26116284291EA48864CF5D9ECCBCB7B03576C79CC0C08A186AE1A3598DA4F264E40FF05302A2308EBB83FA8CCF9C8DAF64012C4B9E013FC4ADB1E13980EA8DA029EADDD83E454F163DE8DBA69D4144A33D0623D4A5822D5E762206004635DA8EA6BA9339C285A3CDDDF32DBAC2CC3F0EBE4D558013391550E25121C5AB0AB8");
		HttpEntity<?> requestEntity = new HttpEntity<>(null, requestHeaders);
		ResponseEntity<byte[]> resposne = restTemplate.exchange(
				supportLink,
				HttpMethod.GET,
				requestEntity,
				byte[].class);

		// Handle 4xx and 5xx
		if(!resposne.getStatusCode().is2xxSuccessful()) {
			throw new RuntimeException(resposne.getStatusCode().toString());
		}

		return resposne.getBody();
	}

}
