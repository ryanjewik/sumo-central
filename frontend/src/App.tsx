//import { useState } from 'react'
import './App.css'
import './styles/globals.css'
//import { TableCard, Table } from './components/application/table/table';
import { RikishiTable } from './components/application/rikishi_table';
import { Button } from "./components/base/buttons/button";
import { RadarChart } from 'recharts';
import KimariteRadarChart from "./components/application/charts/KimariteRadarChart";
import SearchBar from './components/searchbar';
import DividedList from './components/dividedlist';


function App() {
  //const [count, setCount] = useState(0)

  return (
    <>
      <nav className="navbar">
        <div className="navbar-row navbar-row-top">
          <div className="navbar-left">
            <img src="/sumo_logo.png" alt="Sumo Logo" className="navbar-logo" />
            <span className="navbar-title">Sumo App</span>
          </div>
          <SearchBar />
          <div className="navbar-right">
            <button className="navbar-btn">L</button>
            <button className="navbar-btn">A</button>
          </div>
        </div>
        <div className="navbar-row navbar-row-bottom">
          <a className="navbar-link" href="#">Sumo ▼</a>
          <a className="navbar-link" href="#">Discussions</a>
          <a className="navbar-link" href="#">Brackets</a>
          <a className="navbar-link" href="#">Resources</a>
          <a className="navbar-link" href="#">About</a>
        </div>
      </nav>
      <div id="background">
        <div className="content-box">
          <div
            className="left-bar"
            style={{ display: 'flex', flexDirection: 'column', height: '100%' }}
          >
            <div style={{ flex: 1, paddingBottom: '1rem' }}>
                <RikishiTable />
            </div>
            <div style={{ flex: 1, gap: '1rem', display: 'flex', flexDirection: 'column' }}>
              <DividedList></DividedList>
            </div>
            <div style={{ flex: 1, backgroundColor: 'red' }}>train</div>
          </div>
          <div className="main-content">
            <div className="bracket-area">Bracket Area</div>
            <div className="forum-area">
              <div className="forum-post">Post 1</div>
              <div className="forum-post">Post 2</div>
              <div className="forum-post">Post 3</div>
            </div>
            {/* Untitled UI Test Button */}
            <div style={{ marginTop: '2rem' }}>
              {/* If you have the Button component from Untitled UI, use this import: */}
              {/* import { Button } from "@/components/base/buttons/button"; */}
              {/* Example usage: */}
              {/* <Button color="primary">Untitled UI Test Button</Button> */}
              <Button color="primary">Untitled UI Test Button</Button>
            </div>
          </div>
          <div 
            className="right-bar"
            style={{ display: 'flex', flexDirection: 'column', height: '100%' }}
          >
            <KimariteRadarChart />
          </div>
        </div>
      </div>
    </>
  )
}

export default App
