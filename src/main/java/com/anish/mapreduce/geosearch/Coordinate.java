package com.anish.mapreduce.geosearch;

/**
 * A Class for geographical coordinate. Each instance of the class denotes a
 * point on earth.
 * 
 * @author anish
 *
 */
public class Coordinate {
	/**
	 * Store latitude of the point
	 */
	private double lat;

	/**
	 * Store longitude of the point
	 */
	private double lon;

	public Coordinate(double lat, double lon) {
		this.lat = lat;
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public double getLon() {
		return lon;
	}

	public void print() {
		System.out.println("Lat : " + lat + " Lon : " + lon);
	}

	@Override
	public String toString() {
		return "(" + lat + "," + lon + ")";
	}
}
