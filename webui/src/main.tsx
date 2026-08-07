import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { Toaster } from "sonner";
import "./index.css";
import App from "./App";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
      <Toaster
        theme="dark"
        position="bottom-center"
        closeButton
        toastOptions={{
          style: {
            background: "#1c2128",
            border: "1px solid #3d444d",
            color: "#d1d7e0",
            fontFamily: "var(--font-sans)",
            fontSize: "14px",
          },
        }}
      />
    </BrowserRouter>
  </StrictMode>,
);
