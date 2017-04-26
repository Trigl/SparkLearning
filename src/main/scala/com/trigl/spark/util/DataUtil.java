package com.trigl.spark.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;

public class DataUtil {

	/**
	 * 对字符串解压
	 * @param compressedStr
	 * @return
     */
	public static String unzip(String compressedStr) {
		if (compressedStr == null) {
			return null;
		}
		ByteArrayOutputStream out = null;
		ByteArrayInputStream in = null;
		ZipInputStream zin = null;
		String decompressed = null;
		try {
			byte[] compressed = Base64.decodeBase64(compressedStr);
			out = new ByteArrayOutputStream();
			in = new ByteArrayInputStream(compressed);
			zin = new ZipInputStream(in);
			zin.getNextEntry();
			byte[] buffer = new byte[1024];
			int offset = -1;
			while ((offset = zin.read(buffer)) != -1) {
				out.write(buffer, 0, offset);
			}
			decompressed = out.toString();
		} catch (IOException e) {
			decompressed = null;
			e.printStackTrace();
		} finally {
			if (zin != null) {
				try {
					zin.close();
				} catch (IOException e) {
				}
			}
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
		return decompressed;
	}

	/**
	 * 对字符串加密
	 * @param in
	 * @return
     */
	public static String decode(String in){
		if(StringUtils.isBlank(in))
			return null;
		
		int len = in.length();
		
		int remainder = len % 7;
		remainder = remainder <= 3 ? 3 : remainder;
		
		String sub = "";
		StringBuffer sbu = new StringBuffer();
		for (int i = 0; i < len; i += remainder) {
			if (len - i >= remainder) {
				sub = in.substring(i, i + remainder);
			} else {
				sub = in.substring(i, len);
			}
			sbu.append(new StringBuffer(sub).reverse());
		}
		
		String[] unicodes = sbu.toString().split("n");
		
		String text = "";
		for(int i = 1; i < unicodes.length; i++){
		    int hexVal = Integer.parseInt(unicodes[i], 16);
		    text += (char)hexVal;
		}
		
		text = text.replaceAll("@", ".");
		text = text.replaceAll("#", ".");
		text = text.replaceAll("\\$", ".");
		
		text = text.replaceAll("\\*", ";");
		text = text.replaceAll("%", ";");
		
		return text;
	}

	/**
	 * 获取经度
	 * @param geo
	 * @return
     */
	public static double getLongitude(Long geo) {
		if (geo == null || geo == 0L) {
			return 0;
		}
		
		String geoStr = String.format("%018d", geo);

		int idx = 0;
		String a = geoStr.substring(idx, idx += 3);
		String b = geoStr.substring(idx, idx += 6);
		double result = Double.parseDouble(a + "." + b);

		return result;
	}

	/**
	 * 获取纬度
	 * @param geo
	 * @return
     */
	public static double getLatitude(Long geo) {
		if (geo == null || geo == 0L) {
			return 0;
		}
		
		String geoStr = String.format("%018d", geo);
		
		int idx = 9;
		String a = geoStr.substring(idx, idx += 3);
		String b = geoStr.substring(idx, idx += 6);
		double result = Double.parseDouble(a + "." + b);

		return result;
	}
}
