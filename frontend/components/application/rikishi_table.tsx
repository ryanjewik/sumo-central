"use client";

//import React from "react";
import { Table, TableCard } from "./table/table"; // adjust import if path differs

const columns = [
  { id: "profile", label: "Profile" },
  { id: "name", label: "Name" },
  { id: "rank", label: "Rank" },
  { id: "wins", label: "Wins" },
];

interface RikishiTableProps {
  topRikishiOrdered?: any[];
}

const defaultRawData = [
  {
    id: 1,
    name: "Hakuho",
    rank: "Yokozuna",
    wins: 45,
    profile: "../../../../sumo_logo.png",
  },
  {
    id: 2,
    name: "Asanoyama",
    rank: "Ozeki",
    wins: 12,
    profile: "../../../../sumo_logo.png",
  },
];

export function RikishiTable({ topRikishiOrdered }: RikishiTableProps) {
  // map incoming ordered list (from homepage.top_rikishi_ordered) to table rows
  const data = (topRikishiOrdered && Array.isArray(topRikishiOrdered) && topRikishiOrdered.length > 0)
    ? topRikishiOrdered.map((r: any, idx: number) => ({
      id: r.id ?? r._id ?? idx,
      name: r.shikona ?? r.name ?? 'Unknown',
      rank: r.current_rank ?? r.rank ?? '',
      wins: r.wins ?? r.win_count ?? 0,
      profile: (r.photo_url ?? r.profile ?? '/sumo_logo.png'),
      index: idx,
    }))
    : defaultRawData.map((item, idx) => ({ ...item, index: idx }));
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
                key={item.id}
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
                        <img
                          src={(item as any).profile ?? '/sumo_logo.png'}
                          alt={(item as any).name}
                          className="h-8 w-8 rounded-full object-cover"
                          style={{ width: 32, height: 32 }}
                        />
                      </Table.Cell>
                    );
                  }
                  if (column.id === 'name') {
                    return (
                      <Table.Cell className={`${cellBg} ${cellBorder}`}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <a
                            href={`/rikishi/${item.id}`}
                            style={{ textDecoration: 'none', color: 'inherit', display: 'inline-flex', alignItems: 'center', gap: 8 }}
                            title={`View profile for ${(item as any).name}`}
                          >
                            <span style={{ fontWeight: 700 }}>{(item as any).name}</span>
                          </a>
                          <span style={{ fontSize: 12, color: '#563861', background: '#fff2f6', padding: '2px 6px', borderRadius: 8 }}>{(item as any).rank}</span>
                        </div>
                      </Table.Cell>
                    );
                  }
                  return (
                    <Table.Cell className={`${cellBg} ${cellBorder}`}>
                      {(item as any)[column.id]}
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
