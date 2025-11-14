"use client";

//import React from "react";
import { Table, TableCard } from "./table/table"; // adjust import if path differs
import Image from 'next/image';
import Link from 'next/link';

const columns = [
  { id: "profile", label: "Profile" },
  { id: "name", label: "Name" },
  { id: "rank", label: "Rank" },
  { id: "wins", label: "Wins" },
];

interface RikishiTableProps {
  topRikishiOrdered?: Record<string, unknown>[] | Record<string, Record<string, unknown>>;
}

type IncomingRikishi = Record<string, unknown>;
type RikishiRow = {
  id: string | number;
  name: string;
  rank: string;
  wins: number;
  profile: string;
  index: number;
};

const defaultRawData = [
  {
    id: 1,
    name: "Hakuho",
    rank: "Yokozuna",
    wins: 45,
    profile: "/sumo_logo.png",
  },
  {
    id: 2,
    name: "Asanoyama",
    rank: "Ozeki",
    wins: 12,
    profile: "/sumo_logo.png",
  },
];

export function RikishiTable({ topRikishiOrdered }: RikishiTableProps) {
  // map incoming ordered list (from homepage.top_rikishi_ordered) to table rows
  let sourceList: IncomingRikishi[] = [];
  if (topRikishiOrdered) {
    if (Array.isArray(topRikishiOrdered)) {
      sourceList = topRikishiOrdered as IncomingRikishi[];
    } else if (typeof topRikishiOrdered === 'object') {
      // sometimes the backend stores ordered rikishi as an object/map — take values
      sourceList = Object.values(topRikishiOrdered as Record<string, IncomingRikishi>);
    }
  }

  // helper to read possibly-unknown keys safely
  const getString = (o: IncomingRikishi | undefined, ...keys: string[]) => {
    if (!o) return undefined;
    for (const k of keys) {
      const v = o[k];
      if (typeof v === 'string') return v;
      if (typeof v === 'number') return String(v);
    }
    return undefined;
  };

  const getNumber = (o: IncomingRikishi | undefined, ...keys: string[]) => {
    const s = getString(o, ...keys);
    if (typeof s === 'string') return Number(s) || 0;
    return 0;
  };

  const data: RikishiRow[] = (sourceList && sourceList.length > 0)
    ? sourceList.map((r: IncomingRikishi, idx: number) => ({
      id: getString(r, 'id', '_id') ?? idx,
      name: getString(r, 'shikona', 'name', 'display_name') ?? 'Unknown',
      rank: getString(r, 'current_rank', 'rank', 'rank_label') ?? '',
      wins: getNumber(r, 'wins', 'win_count'),
      // prefer S3-hosted image if available (backend may include s3_url), then common keys
      profile: getString(r, 's3_url', 'photo_url', 'profile') ?? '/sumo_logo.png',
      index: idx,
    }))
    : defaultRawData.map((item, idx) => ({ ...item, index: idx })) as RikishiRow[];
  return (
    <div className="w-full h-full app-text text-[1.15rem]">
      <TableCard.Root size="sm" className="bg-[#A3E0B8] border-3 border-[#563861]">
        <TableCard.Header title="Top Rikishi" description="Makuuchi Division" className="bg-[#A3E0B8] font-extrabold text-[1.35rem]" />
        <Table aria-label="Top Rikishi Table" selectionMode="none" className="w-full" role="table">
          <Table.Header columns={columns}>
            {(column) => (
              <Table.Head
                key={column.id}
                label={column.label}
                {...(columns.findIndex((c) => c.id === column.id) === 0 ? { isRowHeader: true } : {})}
              />
            )}
          </Table.Header>
          <Table.Body items={data}>
            {(item) => (
              <Table.Row
                key={`${item.id ?? 'r'}-${item.index}`}
                columns={columns}
                // make rows keyboard accessible; clicking navigates to rikishi page
                className="cursor-pointer"
              >
                {(column) => {
                  const cellBg = item.index % 2 === 0 ? '!bg-[#E0A3C2]' : '!bg-[#E4E0BE]';
                  const cellBorder = 'border-2 border-[#563861]';
                  if (column.id === "profile") {
                    return (
                      <Table.Cell className={`${cellBg} ${cellBorder}`}>
                        <div style={{ width: 40, height: 40, position: 'relative', borderRadius: '50%', overflow: 'hidden', boxShadow: '0 4px 12px rgba(0,0,0,0.06)' }}>
                          <Link href={`/rikishi/${item.id}`} passHref>
                            <a aria-label={`View profile for ${String(item.name)}`} style={{ display: 'block', width: '100%', height: '100%' }}>
                              <Image src={String(item.profile ?? '/sumo_logo.png')} alt={String(item.name)} fill style={{ objectFit: 'cover' }} />
                            </a>
                          </Link>
                        </div>
                      </Table.Cell>
                    );
                  }
                  if (column.id === 'name') {
                    return (
                      <Table.Cell className={`${cellBg} ${cellBorder}`}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <Link href={`/rikishi/${item.id}`} passHref>
                                  <a style={{ textDecoration: 'none', color: 'inherit', display: 'inline-flex', alignItems: 'center', gap: 8 }} title={`View profile for ${String(item.name)}`}>
                                    <span style={{ fontWeight: 700 }}>{String(item.name)}</span>
                                  </a>
                                </Link>
                          <span style={{ fontSize: 12, color: '#563861', background: '#fff2f6', padding: '2px 6px', borderRadius: 8 }}>{String(item.rank)}</span>
                        </div>
                      </Table.Cell>
                    );
                  }
                  return (
                    <Table.Cell className={`${cellBg} ${cellBorder}`}>
                      {(item as RikishiRow)[column.id as keyof RikishiRow]}
                    </Table.Cell>
                  );
                }}
              </Table.Row>
            )}
          </Table.Body>
        </Table>
      </TableCard.Root>
    </div>
  );
}
