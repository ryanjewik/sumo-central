import { useState } from 'react'
import './App.css'
import { Button } from "@/components/base/buttons/button";

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
          <div className="left-bar">Left</div>
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
          <div className="right-bar">Right</div>
        </div>
      </div>
    </>
  )
}

export default App
