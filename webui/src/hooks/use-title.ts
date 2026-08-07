import { useEffect } from "react";

export function useTitle(title: string) {
  useEffect(() => {
    document.title = `yogfile — ${title}`;
  }, [title]);
}
