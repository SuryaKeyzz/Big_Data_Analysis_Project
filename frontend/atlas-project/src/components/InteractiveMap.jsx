import React, { useState } from "react";
import SeaTBMap from "../components/SeaTBMap";

export function InteractiveMapPage() {
  const [selectedYear, setSelectedYear] = useState(2018);
  const [selectedCountry, setSelectedCountry] = useState(null);

  const handleCountryClick = (country) => {
    setSelectedCountry(country); // keep your existing sidebar/detail logic
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold mb-4">Interactive Map</h1>

      <div className="mb-4">
        <label className="mr-2 font-medium">Year:</label>
        <select
          value={selectedYear}
          onChange={(e) => setSelectedYear(Number(e.target.value))}
          className="border rounded px-2 py-1"
        >
          {[2018, 2019, 2020, 2021, 2022, 2023].map((y) => (
            <option key={y} value={y}>
              {y}
            </option>
          ))}
        </select>
      </div>

      <div className="min-h-[520px]">
        <SeaTBMap
          selectedYear={selectedYear}
          selectedCountry={selectedCountry}
          onCountryClick={handleCountryClick}
        />
      </div>

      {selectedCountry && (
        <div className="mt-4 p-4 rounded-lg border">
          <div className="font-semibold">{selectedCountry.country}</div>
          <div className="text-sm text-gray-600">ISO3: {selectedCountry.iso3}</div>
          <div className="mt-2 text-sm">
            Total: {selectedCountry.totalCases?.toLocaleString()} · New: {selectedCountry.newCases?.toLocaleString()} · Deaths: {selectedCountry.deathCases?.toLocaleString()}
          </div>
        </div>
      )}
    </div>
  );
}           