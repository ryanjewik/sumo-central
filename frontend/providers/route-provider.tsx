// providers/route-provider.tsx
'use client';

import { PropsWithChildren, useCallback } from 'react';
import React from 'react';
import { RouterProvider } from 'react-aria-components';
import { useRouter } from 'next/navigation';

// Tell react-aria-components what options our router supports
declare module 'react-aria-components' {
  interface RouterConfig {
    routerOptions: { replace?: boolean };
  }
}

export function RouteProvider({ children }: PropsWithChildren) {
  const router = useRouter();

  const navigate = useCallback(
    (to: string, options?: { replace?: boolean }) => {
      if (options?.replace) router.replace(to);
      else router.push(to);
    },
    [router]
  );

  // Next doesn’t need a custom `useHref`; returning `to` is fine for RAC
  const useHref = useCallback((to: string) => to, []);

  const providerProps = { navigate, useHref, children };
  return (
    <RouterProvider {...(providerProps as React.ComponentProps<typeof RouterProvider>)}>
      {children}
    </RouterProvider>
  );
}

export default RouteProvider;
