import { lazy, Suspense } from "react";
import { Routes, Route } from "react-router";
import { Layout } from "./components/layout/Layout";
import { FilesPage } from "./pages/files";

const DashboardPage = lazy(() =>
  import("./pages/dashboard").then((m) => ({ default: m.DashboardPage })),
);
const DownloadPage = lazy(() =>
  import("./pages/download").then((m) => ({ default: m.DownloadPage })),
);

const loading = <div className="p-6 text-muted">Chargement…</div>;

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route
          path="/dashboard"
          element={
            <Suspense fallback={loading}>
              <DashboardPage />
            </Suspense>
          }
        />
        <Route
          path="/d/:hash"
          element={
            <Suspense fallback={loading}>
              <DownloadPage />
            </Suspense>
          }
        />
        <Route path="*" element={<FilesPage />} />
      </Route>
    </Routes>
  );
}
