import { useState } from 'react'
import './App.css'

function App() {
  const [count, setCount] = useState(0)

  return (
    <>
      <nav className="navbar">
        <div className="navbar-row navbar-row-top">
          <div className="navbar-left">
            <img src="/sumo_logo.png" alt="Sumo Logo" className="navbar-logo" />
            <span className="navbar-title">Sumo App</span>
          </div>
          <input className="navbar-search" type="text" placeholder="search" />
          <div className="navbar-right">
            <button className="navbar-btn">language</button>
            <button className="navbar-btn">acc</button>
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
      <div id="background"></div>
    </>
  )
}

export default App
