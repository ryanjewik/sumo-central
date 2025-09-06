import React from "react";
import type { ButtonHTMLAttributes, ReactNode } from "react";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  color?: "primary" | "secondary" | "tertiary";
  children: ReactNode;
}

export const Button: React.FC<ButtonProps> = ({ color = "primary", children, className = "", ...props }) => {
  let colorClass = "";
  switch (color) {
    case "primary":
      colorClass = "bg-blue-600 text-white hover:bg-blue-700";
      break;
    case "secondary":
      colorClass = "bg-gray-200 text-gray-900 hover:bg-gray-300";
      break;
    case "tertiary":
      colorClass = "bg-transparent text-blue-600 border border-blue-600 hover:bg-blue-50";
      break;
    default:
      colorClass = "bg-blue-600 text-white hover:bg-blue-700";
  }
  return (
    <button
      type="button"
      className={`px-4 py-2 rounded font-semibold transition ${colorClass} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
};
