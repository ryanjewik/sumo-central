//import React from "react";
import { Table, TableCard } from "./table/table"; // adjust import if path differs

const columns = [
  { id: "profile", label: "Profile" },
  { id: "name", label: "Name" },
  { id: "rank", label: "Rank" },
  { id: "wins", label: "Wins" },
];

const rawData = [
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
  {
    id: 3,
    name: "Terunofuji",
    rank: "Yokozuna",
    wins: 20,
    profile: "../../../../sumo_logo.png",
  },
  {
    id: 4,
    name: "Takakeisho",
    rank: "Ozeki",
    wins: 18,
    profile: "../../../../sumo_logo.png",
  },
  {
    id: 5,
    name: "Wakatakakage",
    rank: "Sekiwake",
    wins: 10,
    profile: "../../../../sumo_logo.png",
  },
  {
    id: 6,
    name: "Shodai",
    rank: "Sekiwake",
    wins: 8,
    profile: "../../../../sumo_logo.png",
  },
];

const data = rawData.map((item, idx) => ({ ...item, index: idx }));

export function RikishiTable() {
  return (
    <div className="w-full h-full app-text text-[1.15rem]">
      <TableCard.Root size="sm" className="bg-[#A3E0B8] border-3 border-[#563861]">
        <TableCard.Header title="Top Rikishi" description="Makuuchi Division" className="bg-[#A3E0B8] font-extrabold text-[1.35rem]" />
        <Table aria-label="Top Rikishi Table" selectionMode="none" className="w-full">
          <Table.Header columns={columns}>
            {(column) => <Table.Head key={column.id} label={column.label} />}
          </Table.Header>
          <Table.Body items={data}>
            {(item) => (
              <Table.Row
                key={item.id}
                columns={columns}
              >
                {(column) => {
                  const cellBg = item.index % 2 === 0 ? '!bg-[#E0A3C2]' : '!bg-[#E4E0BE]';
                  const cellBorder = 'border-2 border-[#563861]';
                  if (column.id === "profile") {
                    return (
                      <Table.Cell className={`${cellBg} ${cellBorder}`}>
                        <img
                          src={(item as any).profile}
                          alt={(item as any).name}
                          className="h-8 w-8 rounded-full object-cover max-w-full"
                          style={{ maxWidth: '100%', height: 'auto' }}
                        />
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
