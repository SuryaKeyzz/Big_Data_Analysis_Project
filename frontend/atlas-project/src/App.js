import React, { useState, useEffect } from 'react';
import apiService from './services/apiServices';
import InteractiveMapPage from './components/InteractiveMap';
import CountryInfoBox from './components/CountryInfoBox';
import TrendsChart from './components/TrendsChart';
import ComparisonTable from './components/ComparisonTable';
import './App.css';

function App() {
  const [selectedYear, setSelectedYear] = useState('2023');
  const [selectedCountry, setSelectedCountry] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [mapData, setMapData] = useState(null);
  const [activeTab, setActiveTab] = useState('map');

  const handleCountryClick = (countryData) => {
    setSelectedCountry(countryData);
  };

  const handleCloseInfoBox = () => {
    setSelectedCountry(null);
  };

  const handleYearChange = (year) => {
    setSelectedYear(year);
  };

  // Make handleYearChange available globally for RealWorldMap component
  useEffect(() => {
    window.handleYearChange = handleYearChange;
    return () => {
      delete window.handleYearChange;
    };
  }, []);

  useEffect(() => {
    const fetchMapData = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await apiService.getMapData(selectedYear);
        setMapData(data);
      } catch (err) {
        setError(`Failed to load map data: ${err.message}`);
        console.error('Map data fetch error:', err);
      } finally {
        setLoading(false);
      }
    };

    if (activeTab === 'map') {
      fetchMapData();
    }
  }, [selectedYear, activeTab]);

  const tabs = [
    { id: 'map', label: 'Interactive Map', icon: '🗺️' },
    { id: 'trends', label: 'Yearly Trends', icon: '📈' },
    { id: 'comparison', label: 'Country Comparison', icon: '📊' }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      {/* Header */}
      <div className="bg-white/80 backdrop-blur-sm border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="text-center">
            <h1 className="text-4xl font-bold bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-600 bg-clip-text text-transparent">
              Tuberculosis Data Visualization
            </h1>
            <p className="text-gray-600 mt-2 text-lg">
              Southeast Asia TB Cases and Statistics (2018-2023)
            </p>
          </div>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="max-w-7xl mx-auto px-4 py-4">
        <div className="flex justify-center">
          <div className="bg-white/90 backdrop-blur-sm rounded-xl shadow-lg border border-gray-200 p-1">
            {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-6 py-3 rounded-lg text-sm font-medium transition-all duration-200 flex items-center space-x-2 ${
                  activeTab === tab.id
                    ? 'bg-blue-600 text-white shadow-md'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
              >
                <span>{tab.icon}</span>
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {error && (
        <div className="max-w-7xl mx-auto px-4 mb-4">
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded-lg">
            <strong className="font-bold">Error: </strong>
            <span className="block sm:inline">{error}</span>
          </div>
        </div>
      )}

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 pb-8">
        {/* Map Tab */}
        {activeTab === 'map' && (
          <div className="relative">
            <div className="bg-white/90 backdrop-blur-sm rounded-2xl shadow-2xl border border-gray-200 p-6">
              <div className="mb-4">
                <h2 className="text-2xl font-semibold text-gray-800 mb-2">Interactive Map</h2>
                <p className="text-gray-600">Click on highlighted countries for details</p>
              </div>

              <div className="relative">
                <InteractiveMapPage
                  selectedYear={selectedYear}
                  onCountryClick={handleCountryClick}
                  selectedCountry={selectedCountry}
                  mapData={mapData}
                  loading={loading}
                  error={error}
                />
              </div>
            </div>

            {/* Legend */}
            <div className="fixed bottom-4 left-4 bg-white/90 backdrop-blur-sm rounded-lg shadow-lg border border-gray-200 p-4 max-w-xs">
              <h3 className="font-semibold text-gray-800 mb-3">Legend</h3>
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 rounded" style={{ backgroundColor: 'rgb(252, 141, 89)' }}></div>
                  <span className="text-sm text-gray-700">Southeast Asian Countries</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-red-600 rounded"></div>
                  <span className="text-sm text-gray-700">Higher TB Cases</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-red-200 rounded"></div>
                  <span className="text-sm text-gray-700">Lower TB Cases</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 bg-gray-300 rounded"></div>
                  <span className="text-sm text-gray-700">Other Countries</span>
                </div>
              </div>

              <div className="mt-4 pt-3 border-t border-gray-200">
                <div className="text-xs text-gray-500">
                  Data covers 10 Southeast Asian countries (excluding Brunei) from 2018-2023
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Trends Tab */}
        {activeTab === 'trends' && (
          <TrendsChart />
        )}

        {/* Comparison Tab */}
        {activeTab === 'comparison' && (
          <ComparisonTable selectedYear={selectedYear} />
        )}

        {/* Country Info Box - Overlay */}
        {selectedCountry && (
          <CountryInfoBox
            selectedCountry={selectedCountry}
            onClose={handleCloseInfoBox}
            onYearChange={handleYearChange}
          />
        )}
      </div>
    </div>
  );
}

export default App;
