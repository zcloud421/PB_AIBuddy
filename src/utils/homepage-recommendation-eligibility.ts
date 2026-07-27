type HomepageFlag = {
    type: string;
};

type HomepageWaitReason =
    | 'WAIT_EARNINGS_RISK'
    | 'WAIT_POST_EARNINGS_SHOCK'
    | 'WAIT_SETUP_RESET';

export function hasHomepageWaitContext(input: {
    wait_reason?: HomepageWaitReason | null;
    flags?: HomepageFlag[];
}): boolean {
    if (input.wait_reason) {
        return true;
    }

    return (input.flags ?? []).some(
        (flag) =>
            flag.type === 'EARNINGS_PROXIMITY' ||
            flag.type === 'POST_EARNINGS_SHOCK'
    );
}

