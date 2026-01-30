'use client'

import Link from 'next/link'

export function Header() {
  return (
    <header className="header">
      <Link href="/" className="brand">
        <i className="bi bi-lock-fill brand-icon" />
        730's Locks
      </Link>
    </header>
  )
}
