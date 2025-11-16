// FetchInterceptor was used for local debugging to rewrite loopback URLs and
// log all fetches during development. We've removed its runtime behavior and
// keep a no-op component here so references that weren't removed don't break.
// If you want to permanently delete this file, remove it from the repo.

"use client";
import React from 'react';

export default function FetchInterceptor() {
  // no-op: interceptor removed to avoid confusing rewrites in production
  return null;
}
