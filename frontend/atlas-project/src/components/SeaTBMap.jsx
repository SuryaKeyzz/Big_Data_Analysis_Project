import React, { useEffect, useMemo, useState } from "react";
import { ComposableMap, Geographies, Geography, ZoomableGroup } from "react-simple-maps";
import { scaleLinear } from "d3-scale";
import { interpolateReds } from "d3-scale-chromatic";
import tbData from "../assets/tb_data.json";

// Reliable TopoJSON source
const WORLD_TOPO_110M = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

export default function SeaTBMap({ selectedYear, onCountryClick, selectedCountry }) {
  const [mapData, setMapData] = useState([]);
  const [scaleFn, setScaleFn] = useState(null);

  const seaByIso = useMemo(
    () => ({
      IDN: "Indonesia",
      KHM: "Cambodia",
      LAO: "Lao PDR",
      MYS: "Malaysia",
      MMR: "Myanmar",
      PHL: "Philippines",
      SGP: "Singapore",
      THA: "Thailand",
      TLS: "Timor-Leste",
      VNM: "Viet Nam",
    }),
    []
  );

  const nameAliases = useMemo(
    () => ({
      Indonesia: "Indonesia",
      Cambodia: "Cambodia",
      Laos: "Lao PDR",
      "Lao PDR": "Lao PDR",
      Malaysia: "Malaysia",
      Myanmar: "Myanmar",
      Burma: "Myanmar",
      Philippines: "Philippines",
      Singapore: "Singapore",
      Thailand: "Thailand",
      "Timor-Leste": "Timor-Leste",
      "East Timor": "Timor-Leste",
      Vietnam: "Viet Nam",
      "Viet Nam": "Viet Nam",
    }),
    []
  );

  useEffect(() => {
    const rows = [];
    for (const countryName of Object.keys(tbData)) {
      const yr = tbData[countryName]?.[selectedYear];
      if (!yr) continue;
      rows.push({
        country: countryName,
        iso3: yr.iso3,
        totalCases: yr.total_cases,
        population: yr.population,
        deathCases: yr.death_cases,
        prevalentCases: yr.prevalent_cases,
        newCases: yr.new_cases,
      });
    }
    setMapData(rows);
    if (rows.length) {
      const maxCases = Math.max(...rows.map((d) => d.totalCases || 0));
      setScaleFn(() => scaleLinear().domain([0, maxCases]).range([0, 1]));
    } else {
      setScaleFn(null);
    }
  }, [selectedYear]);

  const getProps = (geo) => {
    const p = geo.properties || {};
    return {
      iso3: p.ISO_A3 || p.iso_a3 || p.ADM0_A3 || p.ISO3 || "",
      name: p.NAME || p.name || p.NAME_EN || p.ADMIN || p.admin || p.SOVEREIGNT || "",
    };
  };

  const isSeaCountry = (geo) => {
    const { iso3, name } = getProps(geo);
    return !!(seaByIso[iso3] || nameAliases[name]);
  };

  const matchData = (geo) => {
    const { iso3, name } = getProps(geo);
    let rec = mapData.find((d) => d.iso3 === iso3);
    if (!rec && nameAliases[name]) {
      const canonical = nameAliases[name];
      rec = mapData.find((d) => d.country === canonical);
    }
    return rec || null;
  };

  const fillColor = (geo) => {
    if (!isSeaCountry(geo)) return "#e5e7eb";
    const rec = matchData(geo);
    if (!rec || !scaleFn) return "rgb(252, 141, 89)";
    const t = scaleFn(rec.totalCases || 0);
    return interpolateReds(0.3 + 0.7 * t);
  };

  const handleClick = (geo) => {
    if (!isSeaCountry(geo)) return;
    const rec = matchData(geo);
    if (rec) onCountryClick?.(rec);
  };

  return (
    <div className="relative w-full" style={{ minHeight: 520 }}>
      <ComposableMap projection="geoMercator" projectionConfig={{ scale: 200, center: [110, 0] }} width={920} height={520}>
        <ZoomableGroup zoom={1.5} center={[110, 0]}>
          <Geographies geography={WORLD_TOPO_110M}>
            {({ geographies }) =>
              geographies.map((geo) => {
                const rec = matchData(geo);
                const selected = selectedCountry && rec && selectedCountry.iso3 === rec.iso3;
                const sea = isSeaCountry(geo);
                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={fillColor(geo)}
                    stroke={sea ? "#4b5563" : "#d1d5db"}
                    strokeWidth={sea ? (selected ? 3 : 2) : 0.5}
                    style={{
                      default: { outline: "none" },
                      hover: {
                        fill: sea ? "#dc2626" : "#f3f4f6",
                        outline: "none",
                        cursor: sea ? "pointer" : "default",
                        stroke: sea ? "#ffffff" : "#d1d5db",
                        strokeWidth: sea ? 3 : 0.5,
                      },
                      pressed: {
                        fill: sea ? "#991b1b" : "#f3f4f6",
                        outline: "none",
                        stroke: sea ? "#ffffff" : "#d1d5db",
                        strokeWidth: sea ? 3 : 0.5,
                      },
                    }}
                    onClick={() => handleClick(geo)}
                  />
                );
              })
            }
          </Geographies>
        </ZoomableGroup>
      </ComposableMap>

      <div className="absolute left-4 bottom-4 bg-white/90 backdrop-blur rounded-2xl shadow p-4 text-sm max-w-xs">
        <div className="font-semibold mb-2">Legend</div>
        <div className="space-y-2">
          <LegendItem swatch="#fca589" label="Southeast Asian Countries" />
          <LegendItem swatch="#7f0a0a" label="Higher TB Cases" />
          <LegendItem swatch="#fca5a5" label="Lower TB Cases" />
          <LegendItem swatch="#e5e7eb" label="Other Countries" />
        </div>
        <div className="mt-3 text-xs text-gray-600">
          Data covers 10 Southeast Asian countries (excluding Brunei) from 2018–2023
        </div>
      </div>
    </div>
  );
}

function LegendItem({ swatch, label }) {
  return (
    <div className="flex items-center gap-2">
      <span className="inline-block w-3.5 h-3.5 rounded" style={{ background: swatch }} />
      <span>{label}</span>
    </div>
  );
}                    