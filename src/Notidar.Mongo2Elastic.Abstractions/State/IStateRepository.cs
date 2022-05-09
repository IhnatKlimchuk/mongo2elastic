namespace Notidar.Mongo2Elastic.State
{
    public interface IStateRepository
    {
        Task<IState?> TryLockStateOrDefaultAsync(CancellationToken cancellationToken = default);
    }
}
