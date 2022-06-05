namespace Notidar.Mongo2Elastic.State
{
    public interface IState : IAsyncDisposable
    {
        string? ResumeToken { get; }
        int Version { get; }
        Task UpdateVersionAsync(int version, CancellationToken cancellationToken = default);
        Task UpdateResumeTokenAsync(string? resumeToken, CancellationToken cancellationToken = default);
    }
}
