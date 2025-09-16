import * as React from 'react';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListDivider from '@mui/joy/ListDivider';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import { Popper } from '@mui/base/Popper';
import { ClickAwayListener } from '@mui/base/ClickAwayListener';
import ListItemDecorator from '@mui/joy/ListItemDecorator';
import KeyboardArrowDown from '@mui/icons-material/KeyboardArrowDown';
import Person from '@mui/icons-material/Person';
import Apps from '@mui/icons-material/Apps';



type Options = {
  initialActiveIndex: null | number;
  vertical: boolean;
  handlers?: {
    onKeyDown: (
      event: React.KeyboardEvent<HTMLAnchorElement>,
      fns: { setActiveIndex: React.Dispatch<React.SetStateAction<number | null>> },
    ) => void;
  };
};

const useRovingIndex = (options?: Options) => {
  const {
    initialActiveIndex = 0,
    vertical = false,
    handlers = {
      onKeyDown: () => {},
    },
  } = options || {};
  const [activeIndex, setActiveIndex] = React.useState<number | null>(
    initialActiveIndex!,
  );
  const targetRefs = React.useRef<Array<HTMLAnchorElement>>([]);
  const targets = targetRefs.current;
  const focusNext = () => {
    let newIndex = activeIndex! + 1;
    if (newIndex >= targets.length) {
      newIndex = 0;
    }
    targets[newIndex]?.focus();
    setActiveIndex(newIndex);
  };
  const focusPrevious = () => {
    let newIndex = activeIndex! - 1;
    if (newIndex < 0) {
      newIndex = targets.length - 1;
    }
    targets[newIndex]?.focus();
    setActiveIndex(newIndex);
  };
  const getTargetProps = (index: number) => ({
    ref: (ref: HTMLAnchorElement) => {
      if (ref) {
        targets[index] = ref;
      }
    },
    tabIndex: activeIndex === index ? 0 : -1,
    onKeyDown: (event: React.KeyboardEvent<HTMLAnchorElement>) => {
      if (Number.isInteger(activeIndex)) {
        if (event.key === (vertical ? 'ArrowDown' : 'ArrowRight')) {
          focusNext();
        }
        if (event.key === (vertical ? 'ArrowUp' : 'ArrowLeft')) {
          focusPrevious();
        }
        handlers.onKeyDown?.(event, { setActiveIndex });
      }
    },
    onClick: () => {
      setActiveIndex(index);
    },
  });
  return {
    activeIndex,
    setActiveIndex,
    targets,
    getTargetProps,
    focusNext,
    focusPrevious,
  };
};

type SumoMenuProps = {
  focusNext: () => void;
  focusPrevious: () => void;
  onMouseEnter?: (event?: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => void;
  onKeyDown?: (event: React.KeyboardEvent<HTMLAnchorElement>) => void;
};

const SumoMenu = React.forwardRef(
  (
    { focusNext, focusPrevious, ...props }: SumoMenuProps,
    ref: React.ForwardedRef<HTMLAnchorElement>,
  ) => {
    const [anchorEl, setAnchorEl] = React.useState<HTMLAnchorElement | null>(null);
    const { targets, setActiveIndex, getTargetProps } = useRovingIndex({
      initialActiveIndex: null,
      vertical: true,
      handlers: {
        onKeyDown: (event, fns) => {
          if (event.key.match(/(ArrowDown|ArrowUp|ArrowLeft|ArrowRight)/)) {
            event.preventDefault();
          }
          if (event.key === 'Tab') {
            setAnchorEl(null);
            fns.setActiveIndex(null);
          }
          if (event.key === 'ArrowLeft') {
            setAnchorEl(null);
            focusPrevious();
          }
          if (event.key === 'ArrowRight') {
            setAnchorEl(null);
            focusNext();
          }
        },
      },
    });

    const open = Boolean(anchorEl);
    const id = open ? 'about-popper' : undefined;
    return (
      <ClickAwayListener onClickAway={() => setAnchorEl(null)}>
        <div onMouseLeave={() => setAnchorEl(null)}>
          <ListItemButton
            aria-haspopup
            aria-expanded={open ? 'true' : 'false'}
            ref={ref}
            {...props}
            role="menuitem"
            onKeyDown={(event) => {
              props.onKeyDown?.(event);
              if (event.key.match(/(ArrowLeft|ArrowRight|Tab)/)) {
                setAnchorEl(null);
              }
              if (event.key === 'ArrowDown') {
                event.preventDefault();
                targets[0]?.focus();
                setActiveIndex(0);
              }
            }}
            onFocus={(event) => setAnchorEl(event.currentTarget)}
            onMouseEnter={(event) => {
              props.onMouseEnter?.(event);
              setAnchorEl(event.currentTarget);
            }}
            sx={[
              { fontSize: '1.25rem', fontWeight: 500, color: '#563861', borderRadius: '0.75rem', transition: 'background 0.22s, color 0.22s' },
              open && {
                background: '#f5e6c8',
                color: '#563861',
                borderRadius: '0.75rem',
              }
            ]}
          >
            Sumo <KeyboardArrowDown />
          </ListItemButton>
          <Popper id={id} open={open} anchorEl={anchorEl} disablePortal keepMounted>
            <List
              role="menu"
              aria-label="About"
              variant="outlined"
              sx={{
                my: 2,
                boxShadow: 'md',
                borderRadius: 'sm',
                '--List-radius': '8px',
                '--List-padding': '4px',
                '--ListDivider-gap': '4px',
                '--ListItemDecorator-size': '32px',
                backgroundColor: 'white',
                minWidth: 120,
                border: '1px solid',
                borderColor: 'neutral.outlinedBorder',
                ...(open && {
                  boxShadow: 'lg',
                  borderColor: 'primary.outlinedBorder',
                }),
              }}
               className="app-text"
            >
              <ListItem role="none">
                <ListItemButton role="menuitem" {...getTargetProps(0)}>
                  <ListItemDecorator>
                    <Person />
                  </ListItemDecorator>
                  Rikishi
                </ListItemButton>
              </ListItem>
              <ListItem role="none">
                <ListItemButton role="menuitem" {...getTargetProps(1)}>
                  <ListItemDecorator>
                    <Apps />
                  </ListItemDecorator>
                  Basho
                </ListItemButton>
              </ListItem>
            </List>
          </Popper>
        </div>
      </ClickAwayListener>
    );
  },
);

export default function HorizontalList() {
    const { targets, getTargetProps, setActiveIndex, focusNext, focusPrevious } =
    useRovingIndex();
  return (
  <div
    className="w-full"
    style={{
      marginLeft: 'clamp(0.5rem, 12vw, 15rem)',
      marginRight: 'clamp(0.5rem, 12vw, 15rem)',
    }}
  >
    <Box component="nav" aria-label="My site" sx={{ width: '100%' }}>
      <List
        role="menubar"
        orientation="horizontal"
        className="flex flex-row justify-between items-center w-full app-text"
      >
        <ListDivider orientation="vertical" />
        <ListItem role="none" className="flex-1 flex justify-center">
          <SumoMenu
            onMouseEnter={() => {
              setActiveIndex(1);
              targets[1].focus();
            }}
            focusNext={focusNext}
            focusPrevious={focusPrevious}
            {...getTargetProps(1)}
          />
        </ListItem>
        <ListDivider orientation="vertical" />
        <ListItem role="none" className="flex-1 flex justify-center">
          <ListItemButton
            role="menuitem"
            component="a"
            href="#horizontal-list"
            className="modern-navbar-link"
            sx={{
              position: 'relative',
              color: '#563861',
              fontSize: '1.25rem',
              fontWeight: 500,
              pb: '2px',
              overflow: 'hidden',
              borderRadius: '0.75rem',
              transition: 'background 0.22s, color 0.22s',
              '::after': {
                content: '""',
                position: 'absolute',
                left: 0,
                bottom: 0,
                width: 0,
                height: '2px',
                background: 'linear-gradient(90deg, #e0a3c2 0%, #563861 100%)',
                transition: 'width 0.35s cubic-bezier(0.77,0,0.175,1)',
              },
              '&:hover::after': {
                width: '100%',
              },
              '&:hover': {
                color: '#563861',
                background: '#f5e6c8',
                borderRadius: '0.75rem',
              },
            }}
          >
            Discussions
          </ListItemButton>
        </ListItem>
        <ListDivider orientation="vertical" />
        <ListItem role="none" className="flex-1 flex justify-center">
          <ListItemButton
            role="menuitem"
            component="a"
            href="#horizontal-list"
            className="modern-navbar-link"
            sx={{
              position: 'relative',
              color: '#563861',
              fontSize: '1.25rem',
              fontWeight: 500,
              pb: '2px',
              overflow: 'hidden',
              borderRadius: '0.75rem',
              transition: 'background 0.22s, color 0.22s',
              '::after': {
                content: '""',
                position: 'absolute',
                left: 0,
                bottom: 0,
                width: 0,
                height: '2px',
                background: 'linear-gradient(90deg, #e0a3c2 0%, #563861 100%)',
                transition: 'width 0.35s cubic-bezier(0.77,0,0.175,1)',
              },
              '&:hover::after': {
                width: '100%',
              },
              '&:hover': {
                color: '#563861',
                background: '#f5e6c8',
                borderRadius: '0.75rem',
              },
            }}
          >
            Brackets
          </ListItemButton>
        </ListItem>
        <ListDivider orientation="vertical" />
        <ListItem role="none" className="flex-1 flex justify-center">
          <ListItemButton
            role="menuitem"
            component="a"
            href="#horizontal-list"
            className="modern-navbar-link"
            sx={{
              position: 'relative',
              color: '#563861',
              fontSize: '1.25rem',
              fontWeight: 500,
              pb: '2px',
              overflow: 'hidden',
              borderRadius: '0.75rem',
              transition: 'background 0.22s, color 0.22s',
              '::after': {
                content: '""',
                position: 'absolute',
                left: 0,
                bottom: 0,
                width: 0,
                height: '2px',
                background: 'linear-gradient(90deg, #e0a3c2 0%, #563861 100%)',
                transition: 'width 0.35s cubic-bezier(0.77,0,0.175,1)',
              },
              '&:hover::after': {
                width: '100%',
              },
              '&:hover': {
                color: '#563861',
                background: '#f5e6c8',
                borderRadius: '0.75rem',
              },
            }}
          >
            Resources
          </ListItemButton>
        </ListItem>
        <ListDivider orientation="vertical" />
        <ListItem role="none" className="flex-1 flex justify-center">
          <ListItemButton
            role="menuitem"
            component="a"
            href="#horizontal-list"
            className="modern-navbar-link"
            sx={{
              position: 'relative',
              color: '#563861',
              fontSize: '1.25rem',
              fontWeight: 500,
              pb: '2px',
              overflow: 'hidden',
              borderRadius: '0.75rem',
              transition: 'background 0.22s, color 0.22s',
              '::after': {
                content: '""',
                position: 'absolute',
                left: 0,
                bottom: 0,
                width: 0,
                height: '2px',
                background: 'linear-gradient(90deg, #e0a3c2 0%, #563861 100%)',
                transition: 'width 0.35s cubic-bezier(0.77,0,0.175,1)',
              },
              '&:hover::after': {
                width: '100%',
              },
              '&:hover': {
                color: '#563861',
                background: '#f5e6c8',
                borderRadius: '0.75rem',
              },
            }}
          >
            About
          </ListItemButton>
        </ListItem>
        <ListDivider orientation="vertical" />
      </List>
    </Box>
    
    </div>
  );
}
